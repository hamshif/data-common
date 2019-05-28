from sqlalchemy import Column, Integer, String
from data_common.dal.session_factory import Base


class IngestionRuntimeParams(Base):
    __tablename__ = 'mysql_ingestion_runtime_params'

    namespace_id = Column(Integer, primary_key=True)
    entity = Column(String, primary_key=True)
    manual_loading_start_date = Column(String)
    manual_loading_end_date = Column(String)

    def __repr__(self):
        return "<IngestionStates(namespace_id='%s', entity='%s', manual_loading_start_date='%s', manual_loading_end_date='%s')>" % (
            self.namespace_id, self.entity, self.manual_loading_start_date, self.manual_loading_end_date)
