from itertools import groupby
from operator import itemgetter
from sqlalchemy import and_, or_
from data_common.dal.session_factory import executor
from data_common.dal.table.account_config import AccountConfig
from data_common.dal.table.mysql_ingestion_runtime_params import IngestionRuntimeParams
from data_common.dal.table.mysql_ingestion_states import IngestionStates
from data_common.dal.table.base_entities import BaseEntities
from data_common.dictionary import dictionary as d
from data_common.config.configurer import get_conf


def get_mysql_ingestion_config_grouped_by_namespace():
    subscription = {}
    with executor() as execute:
        results = execute.query(AccountConfig.namespace_id, AccountConfig.entity, AccountConfig.json_args, IngestionStates.last_incremental_date_value, IngestionRuntimeParams.manual_loading_start_date, IngestionRuntimeParams.manual_loading_end_date).\
            filter(and_(AccountConfig.namespace_id == IngestionStates.namespace_id, AccountConfig.namespace_id == IngestionRuntimeParams.namespace_id)).\
            filter(and_(AccountConfig.entity == IngestionStates.entity, AccountConfig.entity == IngestionRuntimeParams.entity)). \
            filter(and_(AccountConfig.source == d.MYSQL, AccountConfig.is_active))

    for result in results:
        subscription.setdefault(result.namespace_id, []).\
            append({'tbl_name': result.entity,
                    'json_args': result.json_args,
                    'last_incremental_date_value': result.last_incremental_date_value,
                    'manual_loading_start_date': result.manual_loading_start_date,
                    'manual_loading_end_date': result.manual_loading_end_date})
    return subscription


def get_kafka_ingestion_config():
    with executor() as execute:
        configs = execute.query(AccountConfig.namespace_id, AccountConfig.entity). \
            filter(and_(AccountConfig.source == d.KAFKA, AccountConfig.is_active))

    return configs


def get_spark_config(etl_type):
    # TODO group_by in alchemy query is more efficient and elegant, in a hurry

    with executor() as execute:

        if etl_type is d.REFINE:

            result = execute.query(AccountConfig).\
                filter(and_(AccountConfig.job_type == d.PIPELINE_REFINE,
                            AccountConfig.is_active)
                       )

        elif etl_type is d.INGESTION:
            result = execute.query(AccountConfig). \
                filter(and_(AccountConfig.job_type == d.PIPELINE_INGESTION,
                            AccountConfig.is_active)
                       )

        else:

            print(f"Snafoo! I don't know what etl_type is: {etl_type}\nReturning!!")
            return

        result_list = [row for row in result]

        namespaces = {}

        for r in result_list:

            namespace = r.namespace_id
            if namespace in namespaces:
                namespaces[namespace].append(r)
            else:
                namespaces[namespace] = [r]

            # print(type(r))
            #
            # print(f"{r.namespace_id}")

    return namespaces


def update_start_process(date, namespace_id, entity):
    update_mysql_ingestion_states_table_column(date, namespace_id, entity, d.START_PROCESS_DATE)


def update_end_process(date, namespace_id, entity):
    update_mysql_ingestion_states_table_column(date, namespace_id, entity, d.END_PROCESS_DATE)


def update_last_incremental(date, namespace_id, entity):
    update_mysql_ingestion_states_table_column(date, namespace_id, entity, d.LAST_INCREMENTAL_DATE_VALUE)


def update_mysql_ingestion_states_table_column(date, namespace_id, entity, column):
    with executor() as execute:
        execute.query(IngestionStates). \
            filter(IngestionStates.namespace_id == namespace_id). \
            filter(IngestionStates.entity == entity).\
            update({column: date})
        execute.commit()


def update_manual_loading_date_range(start_date, end_date, namespace_id, entity):
    with executor() as execute:
        execute.query(IngestionRuntimeParams). \
            filter(IngestionRuntimeParams.namespace_id == namespace_id). \
            filter(IngestionRuntimeParams.entity == entity). \
            update({d.MANUAL_LOADING_START_DATE: start_date, d.MANUAL_LOADING_END_DATE: end_date})
        execute.commit()


def get_base_entities():
    with executor() as execute:
        entities = execute.query(BaseEntities)
    return entities


def onboarding_new_subscription(namespace_id):
    account_config_list = []
    ingestion_runtime_params_list = []
    ingestion_states_list = []

    for entity in get_base_entities():
        account_config_list.append(AccountConfig(
            namespace_id=namespace_id,
            entity=entity.entity_name,
            job_type=entity.job_type,
            source=entity.source,
            destination=entity.destination,
            json_args=entity.json_args,
            is_active=True)
        )

        if entity.source == 'mysql':
            ingestion_states_list.append(
                IngestionStates(
                    namespace_id=namespace_id,
                    entity=entity.entity_name,
                    last_incremental_date_value='1970-01-01 00:00:00')
            )

            ingestion_runtime_params_list.append(
                IngestionRuntimeParams(
                    namespace_id=namespace_id,
                    entity=entity.entity_name
                )
            )

    with executor() as execute:
        execute.add_all(account_config_list)
        execute.add_all(ingestion_states_list)
        execute.add_all(ingestion_runtime_params_list)
        execute.commit()


# TODO make namespace_id and entity unique-together
def upsert_account_config_action(ac):

    with executor() as execute:
        result = execute.query(AccountConfig). \
            filter(and_(AccountConfig.namespace_id == ac.namespace_id,
                        AccountConfig.entity == ac.entity)
                   )\
            .first()

        if not result:

            result = ac

        else:
            result.is_active = ac.is_active

        execute.add(result)
        execute.commit()


def disable_subscription(namespace_id):
    with executor() as execute:
        execute.query(AccountConfig). \
            filter(AccountConfig.namespace_id == namespace_id). \
            update({d.IS_ACTIVE: False})
        execute.commit()


def remove_subscription_conf(namespace_id):
    with executor() as execute:
        execute.query(AccountConfig).filter(AccountConfig.namespace_id == namespace_id).delete()
        execute.query(IngestionStates).filter(IngestionStates.namespace_id == namespace_id).delete()
        execute.query(IngestionRuntimeParams).filter(IngestionRuntimeParams.namespace_id == namespace_id).delete()
        execute.commit()


if __name__ == "__main__":

    _conf = get_conf()
    _namespaces = _conf.namespaces

    for _namespace in _namespaces:

        kafka_account_config = AccountConfig(
            namespace_id=_namespace,
            source=d.KAFKA,
            destination=d.BUCKET_RAW,
            entity=d.ACTIVITY_KAFKA_INGESTION,
            job_type=d.DATAPROC_SPARK,
            json_args={},
            is_active=True
        )

        upsert_account_config_action(kafka_account_config)

        mysql_account_config = AccountConfig(
            namespace_id=_namespace,
            source=d.MYSQL,
            destination=d.BUCKET_RAW,
            entity=d.ACTIVITY_MYSQL_INGESTION,
            job_type=d.DATAPROC_SPARK,
            json_args={},
            is_active=True
        )

        upsert_account_config_action(mysql_account_config)

