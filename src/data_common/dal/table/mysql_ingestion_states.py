from sqlalchemy import Column, Integer, String
from data_common.dal.session_factory import Base


class IngestionStates(Base):
    __tablename__ = 'mysql_ingestion_states'

    namespace_id = Column(Integer, primary_key=True)
    entity = Column(String, primary_key=True)
    start_process_date = Column(String)
    end_process_date = Column(String)
    last_incremental_date_value = Column(String)

    def __repr__(self):
        return "<IngestionStates(namespace_id='%s', entity='%s', start_process_date='%s', end_process_date='%s', last_incremental_date_value='%s')>" % (
            self.namespace_id, self.entity, self.start_process_date, self.end_process_date, self.last_incremental_date_value)
