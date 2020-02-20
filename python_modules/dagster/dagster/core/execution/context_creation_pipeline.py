import sys
from collections import deque, namedtuple
from contextlib import contextmanager

from dagster import check
from dagster.core.definitions.handle import ExecutionTargetHandle
from dagster.core.definitions.pipeline import PipelineDefinition
from dagster.core.definitions.resource import ScopedResourcesBuilder
from dagster.core.definitions.system_storage import SystemStorageData
from dagster.core.engine.init import InitExecutorContext
from dagster.core.errors import (
    DagsterError,
    DagsterResourceFunctionError,
    DagsterUserCodeExecutionError,
    user_code_error_boundary,
)
from dagster.core.events import DagsterEvent, PipelineInitFailureData
from dagster.core.execution.config import ExecutorConfig
from dagster.core.execution.plan.objects import StepInputSourceType
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.instance import DagsterInstance
from dagster.core.log_manager import DagsterLogManager
from dagster.core.storage.init import InitSystemStorageContext
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.core.storage.type_storage import construct_type_storage_plugin_registry
from dagster.core.system_config.objects import EnvironmentConfig
from dagster.loggers import default_loggers, default_system_loggers
from dagster.utils import EventGenerationManager, ensure_gen, merge_dicts
from dagster.utils.error import serializable_error_info_from_exc_info

from .context.init import InitResourceContext
from .context.logger import InitLoggerContext
from .context.system import SystemPipelineExecutionContext, SystemPipelineExecutionContextData


def construct_system_storage_data(storage_init_context):
    return storage_init_context.system_storage_def.system_storage_creation_fn(storage_init_context)


def system_storage_def_from_config(mode_definition, environment_config):
    for system_storage_def in mode_definition.system_storage_defs:
        if system_storage_def.name == environment_config.storage.system_storage_name:
            return system_storage_def

    check.failed(
        'Could not find storage mode {}. Should have be caught by config system'.format(
            environment_config.storage.system_storage_name
        )
    )


def construct_executor_config(executor_init_context):
    return executor_init_context.executor_def.executor_creation_fn(executor_init_context)


def executor_def_from_config(mode_definition, environment_config):
    for executor_def in mode_definition.executor_defs:
        if executor_def.name == environment_config.execution.execution_engine_name:
            return executor_def

    check.failed(
        'Could not find executor {}. Should have be caught by config system'.format(
            environment_config.execution.execution_engine_name
        )
    )


# This represents all the data that is passed *into* context creation process.
# The remainder of the objects generated (e.g. loggers, resources) are created
# using user-defined code that may fail at runtime and result in the emission
# of a pipeline init failure events. The data in this object are passed all
# over the place during the context creation process so grouping here for
# ease of argument passing etc.
ContextCreationData = namedtuple(
    'ContextCreationData',
    'pipeline_def environment_config pipeline_run mode_def system_storage_def '
    'execution_target_handle executor_def instance resource_keys_to_init',
)


def get_required_resource_keys_to_init(execution_plan, system_storage_def):
    resource_keys = set()

    resource_keys = resource_keys.union(system_storage_def.required_resource_keys)

    for step_key, step in execution_plan.step_dict.items():
        if step_key not in execution_plan.step_keys_to_execute:
            continue
        resource_keys = resource_keys.union(
            get_required_resource_keys_for_step(
                step, execution_plan.pipeline_def, system_storage_def
            )
        )

    return frozenset(resource_keys)


def get_required_resource_keys_for_step(execution_step, pipeline_def, system_storage_def):
    resource_keys = set()

    # add all the system storage resource keys
    resource_keys = resource_keys.union(system_storage_def.required_resource_keys)
    solid_def = pipeline_def.get_solid(execution_step.solid_handle).definition

    # add all the solid compute resource keys
    resource_keys = resource_keys.union(solid_def.required_resource_keys)

    # add input type and input hydration config resource keys
    for step_input in execution_step.step_inputs:
        resource_keys = resource_keys.union(step_input.runtime_type.required_resource_keys)
        if (
            step_input.source_type == StepInputSourceType.CONFIG
            and step_input.runtime_type.input_hydration_config
        ):
            resource_keys = resource_keys.union(
                step_input.runtime_type.input_hydration_config.required_resource_keys()
            )

    # add output type and output materialization config resource keys
    for step_output in execution_step.step_outputs:
        resource_keys = resource_keys.union(step_output.runtime_type.required_resource_keys)
        if (
            step_output.should_materialize
            and step_output.runtime_type.output_materialization_config
        ):
            resource_keys = resource_keys.union(
                step_output.runtime_type.output_materialization_config.required_resource_keys()
            )

    # add all the storage-compatible plugin resource keys
    for runtime_type in solid_def.all_runtime_types():
        for auto_plugin in runtime_type.auto_plugins:
            if auto_plugin.compatible_with_storage_def(system_storage_def):
                resource_keys = resource_keys.union(auto_plugin.required_resource_keys())

    return frozenset(resource_keys)


def create_context_creation_data(
    pipeline_def, environment_dict, pipeline_run, instance, execution_plan
):
    environment_config = EnvironmentConfig.build(pipeline_def, environment_dict, pipeline_run)

    mode_def = pipeline_def.get_mode_definition(pipeline_run.mode)
    system_storage_def = system_storage_def_from_config(mode_def, environment_config)
    executor_def = executor_def_from_config(mode_def, environment_config)

    execution_target_handle, _ = ExecutionTargetHandle.get_handle(pipeline_def)

    return ContextCreationData(
        pipeline_def=pipeline_def,
        environment_config=environment_config,
        pipeline_run=pipeline_run,
        mode_def=mode_def,
        system_storage_def=system_storage_def,
        execution_target_handle=execution_target_handle,
        executor_def=executor_def,
        instance=instance,
        resource_keys_to_init=get_required_resource_keys_to_init(
            execution_plan, system_storage_def
        ),
    )


def pipeline_initialization_manager(
    pipeline_def,
    environment_dict,
    pipeline_run,
    instance,
    execution_plan,
    scoped_resources_builder_cm=None,
    system_storage_data=None,
    raise_on_error=False,
):
    scoped_resources_builder_cm = check.opt_callable_param(
        scoped_resources_builder_cm,
        'scoped_resources_builder_cm',
        default=resource_initialization_manager,
    )
    generator = pipeline_initialization_event_generator(
        pipeline_def,
        environment_dict,
        pipeline_run,
        instance,
        execution_plan,
        scoped_resources_builder_cm,
        system_storage_data,
        raise_on_error,
    )
    return EventGenerationManager(generator, SystemPipelineExecutionContext, raise_on_error)


def pipeline_initialization_event_generator(
    pipeline_def,
    environment_dict,
    pipeline_run,
    instance,
    execution_plan,
    scoped_resources_builder_cm,
    system_storage_data=None,
    raise_on_error=False,
):
    pipeline_def = check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition)
    environment_dict = check.dict_param(environment_dict, 'environment_dict', key_type=str)
    pipeline_run = check.inst_param(pipeline_run, 'pipeline_run', PipelineRun)
    instance = check.inst_param(instance, 'instance', DagsterInstance)
    execution_plan = check.inst_param(execution_plan, 'execution_plan', ExecutionPlan)
    scoped_resources_builder_cm = check.callable_param(
        scoped_resources_builder_cm, 'scoped_resources_builder_cm'
    )
    system_storage_data = check.opt_inst_param(
        system_storage_data, 'system_storage_data', SystemStorageData
    )
    raise_on_error = check.bool_param(raise_on_error, 'raise_on_error')

    pipeline_context = None
    resources_manager = None

    try:
        context_creation_data = create_context_creation_data(
            pipeline_def, environment_dict, pipeline_run, instance, execution_plan,
        )
        executor_config = create_executor_config(context_creation_data)
        log_manager = create_log_manager(context_creation_data)
        resources_manager = scoped_resources_builder_cm(
            context_creation_data.pipeline_def,
            context_creation_data.environment_config,
            context_creation_data.pipeline_run,
            log_manager,
            context_creation_data.resource_keys_to_init,
        )
        for event in resources_manager.generate_setup_events():
            yield event
        scoped_resources_builder = check.inst(
            resources_manager.get_object(), ScopedResourcesBuilder
        )
        system_storage_data = create_system_storage_data(
            context_creation_data, system_storage_data, scoped_resources_builder
        )
        pipeline_context = construct_pipeline_execution_context(
            context_creation_data=context_creation_data,
            scoped_resources_builder=scoped_resources_builder,
            system_storage_data=system_storage_data,
            log_manager=log_manager,
            executor_config=executor_config,
            raise_on_error=raise_on_error,
        )
        yield pipeline_context
        for event in resources_manager.generate_teardown_events():
            yield event
    except DagsterError as dagster_error:
        if pipeline_context is None:
            user_facing_exc_info = (
                # pylint does not know original_exc_info exists is is_user_code_error is true
                # pylint: disable=no-member
                dagster_error.original_exc_info
                if dagster_error.is_user_code_error
                else sys.exc_info()
            )

            error_info = serializable_error_info_from_exc_info(user_facing_exc_info)
            yield DagsterEvent.pipeline_init_failure(
                pipeline_name=pipeline_def.name,
                failure_data=PipelineInitFailureData(error=error_info),
                log_manager=_create_context_free_log_manager(instance, pipeline_run, pipeline_def),
            )

        if resources_manager:
            for event in resources_manager.generate_teardown_events():
                yield event

        if pipeline_context or raise_on_error:
            # if we've caught an error after context init we're in a problematic state and
            # should just raise
            raise dagster_error


def create_system_storage_data(
    context_creation_data, system_storage_data, scoped_resources_builder
):
    check.inst_param(context_creation_data, 'context_creation_data', ContextCreationData)

    environment_config, pipeline_def, system_storage_def, pipeline_run = (
        context_creation_data.environment_config,
        context_creation_data.pipeline_def,
        context_creation_data.system_storage_def,
        context_creation_data.pipeline_run,
    )

    system_storage_data = (
        system_storage_data
        if system_storage_data
        else construct_system_storage_data(
            InitSystemStorageContext(
                pipeline_def=pipeline_def,
                mode_def=context_creation_data.mode_def,
                system_storage_def=system_storage_def,
                system_storage_config=environment_config.storage.system_storage_config,
                pipeline_run=pipeline_run,
                instance=context_creation_data.instance,
                environment_config=environment_config,
                type_storage_plugin_registry=construct_type_storage_plugin_registry(
                    pipeline_def, system_storage_def
                ),
                resources=scoped_resources_builder.build(
                    context_creation_data.system_storage_def.required_resource_keys,
                ),
            )
        )
    )

    return system_storage_data


def create_executor_config(context_creation_data):
    check.inst_param(context_creation_data, 'context_creation_data', ContextCreationData)

    return construct_executor_config(
        InitExecutorContext(
            pipeline_def=context_creation_data.pipeline_def,
            mode_def=context_creation_data.mode_def,
            executor_def=context_creation_data.executor_def,
            pipeline_run=context_creation_data.pipeline_run,
            environment_config=context_creation_data.environment_config,
            executor_config=context_creation_data.environment_config.execution.execution_engine_config,
            system_storage_def=context_creation_data.system_storage_def,
            instance=context_creation_data.instance,
        )
    )


def construct_pipeline_execution_context(
    context_creation_data,
    scoped_resources_builder,
    system_storage_data,
    log_manager,
    executor_config,
    raise_on_error,
):
    check.inst_param(context_creation_data, 'context_creation_data', ContextCreationData)
    scoped_resources_builder = check.inst_param(
        scoped_resources_builder if scoped_resources_builder else ScopedResourcesBuilder(),
        'scoped_resources_builder',
        ScopedResourcesBuilder,
    )
    check.inst_param(system_storage_data, 'system_storage_data', SystemStorageData)
    check.inst_param(log_manager, 'log_manager', DagsterLogManager)
    check.inst_param(executor_config, 'executor_config', ExecutorConfig)

    return SystemPipelineExecutionContext(
        SystemPipelineExecutionContextData(
            pipeline_def=context_creation_data.pipeline_def,
            mode_def=context_creation_data.mode_def,
            system_storage_def=context_creation_data.system_storage_def,
            pipeline_run=context_creation_data.pipeline_run,
            scoped_resources_builder=scoped_resources_builder,
            environment_config=context_creation_data.environment_config,
            instance=context_creation_data.instance,
            intermediates_manager=system_storage_data.intermediates_manager,
            file_manager=system_storage_data.file_manager,
            execution_target_handle=context_creation_data.execution_target_handle,
            executor_config=executor_config,
            raise_on_error=raise_on_error,
        ),
        log_manager=log_manager,
    )


def resource_initialization_manager(
    pipeline_def, environment_config, pipeline_run, log_manager, resource_keys_to_init,
):
    generator = resource_initialization_event_generator(
        pipeline_def, environment_config, pipeline_run, log_manager, resource_keys_to_init,
    )
    return EventGenerationManager(generator, ScopedResourcesBuilder)


def resource_initialization_event_generator(
    pipeline_def, environment_config, pipeline_run, log_manager, resource_keys_to_init
):
    check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition)
    check.inst_param(environment_config, 'environment_config', EnvironmentConfig)
    check.inst_param(pipeline_run, 'pipeline_run', PipelineRun)
    check.inst_param(log_manager, 'log_manager', DagsterLogManager)
    check.set_param(resource_keys_to_init, 'resource_keys_to_init', of_type=str)

    resource_instances = {}
    mode_definition = pipeline_def.get_mode_definition(pipeline_run.mode)
    resource_managers = deque()
    generator_closed = False

    try:
        for resource_name, resource_def in sorted(mode_definition.resource_defs.items()):
            if not resource_name in resource_keys_to_init:
                continue
            resource_context = InitResourceContext(
                pipeline_def=pipeline_def,
                resource_def=resource_def,
                resource_config=environment_config.resources.get(resource_name, {}).get('config'),
                run_id=pipeline_run.run_id,
                log_manager=log_manager,
            )
            manager = single_resource_generation_manager(
                resource_context, resource_name, resource_def
            )
            for event in manager.generate_setup_events():
                if event:
                    yield event
            initialized_resource = check.inst(manager.get_object(), InitializedResource)
            resource_instances[resource_name] = initialized_resource.resource
            resource_managers.append(manager)

        yield ScopedResourcesBuilder(resource_instances)
    except GeneratorExit:
        # Shouldn't happen, but avoid runtime-exception in case this generator gets GC-ed
        # (see https://amir.rachum.com/blog/2017/03/03/generator-cleanup/).
        generator_closed = True
        raise
    finally:
        if not generator_closed:
            error = None
            while len(resource_managers) > 0:
                manager = resource_managers.pop()
                try:
                    for event in manager.generate_teardown_events():
                        yield event
                except DagsterUserCodeExecutionError as dagster_user_error:
                    error = dagster_user_error
            if error:
                raise error


class InitializedResource(object):
    ''' Utility class to wrap the untyped resource object emitted from the user-supplied
    resource function.  Used for distinguishing from the framework-yielded events in an
    `EventGenerationManager`-wrapped event stream.
    '''

    def __init__(self, obj):
        self.resource = obj


def single_resource_generation_manager(context, resource_name, resource_def):
    generator = single_resource_event_generator(context, resource_name, resource_def)
    return EventGenerationManager(generator, InitializedResource)


def single_resource_event_generator(context, resource_name, resource_def):
    try:
        msg_fn = lambda: 'Error executing resource_fn on ResourceDefinition {name}'.format(
            name=resource_name
        )
        with user_code_error_boundary(DagsterResourceFunctionError, msg_fn):
            try:
                resource_or_gen = resource_def.resource_fn(context)
                gen = ensure_gen(resource_or_gen)
                resource = next(gen)
                yield InitializedResource(resource)
            except StopIteration:
                check.failed(
                    'Resource generator {name} must yield one item.'.format(name=resource_name)
                )
            try:
                next(gen)
            except StopIteration:
                pass
            else:
                check.failed(
                    'Resource generator {name} yielded more than one item.'.format(
                        name=resource_name
                    )
                )
    except DagsterUserCodeExecutionError as dagster_user_error:
        raise dagster_user_error


@contextmanager
def scoped_pipeline_context(
    pipeline_def,
    environment_dict,
    pipeline_run,
    instance,
    execution_plan,
    scoped_resources_builder_cm=resource_initialization_manager,
    system_storage_data=None,
    raise_on_error=False,
):
    check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition)
    check.dict_param(environment_dict, 'environment_dict', key_type=str)
    check.inst_param(pipeline_run, 'pipeline_run', PipelineRun)
    check.inst_param(instance, 'instance', DagsterInstance)
    check.inst_param(execution_plan, 'execution_plan', ExecutionPlan)
    check.callable_param(scoped_resources_builder_cm, 'scoped_resources_builder_cm')
    check.opt_inst_param(system_storage_data, 'system_storage_data', SystemStorageData)

    initialization_manager = pipeline_initialization_manager(
        pipeline_def,
        environment_dict,
        pipeline_run,
        instance,
        execution_plan,
        scoped_resources_builder_cm=scoped_resources_builder_cm,
        system_storage_data=system_storage_data,
        raise_on_error=raise_on_error,
    )
    for _ in initialization_manager.generate_setup_events():
        pass

    yield check.inst(initialization_manager.get_object(), SystemPipelineExecutionContext)
    for _ in initialization_manager.generate_teardown_events():
        pass


def create_log_manager(context_creation_data):
    check.inst_param(context_creation_data, 'context_creation_data', ContextCreationData)

    pipeline_def, mode_def, environment_config, pipeline_run = (
        context_creation_data.pipeline_def,
        context_creation_data.mode_def,
        context_creation_data.environment_config,
        context_creation_data.pipeline_run,
    )

    loggers = []
    for logger_key, logger_def in mode_def.loggers.items() or default_loggers().items():
        if logger_key in environment_config.loggers:
            loggers.append(
                logger_def.logger_fn(
                    InitLoggerContext(
                        environment_config.loggers.get(logger_key, {}).get('config'),
                        pipeline_def,
                        logger_def,
                        pipeline_run.run_id,
                    )
                )
            )

    if not loggers:
        for (logger_def, logger_config) in default_system_loggers():
            loggers.append(
                logger_def.logger_fn(
                    InitLoggerContext(logger_config, pipeline_def, logger_def, pipeline_run.run_id)
                )
            )

    # should this be first in loggers list?
    loggers.append(context_creation_data.instance.get_logger())

    return DagsterLogManager(
        run_id=pipeline_run.run_id,
        logging_tags=get_logging_tags(pipeline_run, context_creation_data.pipeline_def),
        loggers=loggers,
    )


def _create_context_free_log_manager(instance, pipeline_run, pipeline_def):
    '''In the event of pipeline initialization failure, we want to be able to log the failure
    without a dependency on the ExecutionContext to initialize DagsterLogManager.
    Args:
        pipeline_run (dagster.core.storage.pipeline_run.PipelineRun)
        pipeline_def (dagster.definitions.PipelineDefinition)
    '''
    check.inst_param(instance, 'instance', DagsterInstance)
    check.inst_param(pipeline_run, 'pipeline_run', PipelineRun)
    check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition)

    loggers = [instance.get_logger()]
    # Use the default logger
    for (logger_def, logger_config) in default_system_loggers():
        loggers += [
            logger_def.logger_fn(
                InitLoggerContext(logger_config, pipeline_def, logger_def, pipeline_run.run_id)
            )
        ]

    return DagsterLogManager(
        pipeline_run.run_id, get_logging_tags(pipeline_run, pipeline_def), loggers
    )


def get_logging_tags(pipeline_run, pipeline):
    check.opt_inst_param(pipeline_run, 'pipeline_run', PipelineRun)
    check.inst_param(pipeline, 'pipeline', PipelineDefinition)

    return merge_dicts(
        {'pipeline': pipeline.name}, pipeline_run.tags if pipeline_run and pipeline_run.tags else {}
    )
