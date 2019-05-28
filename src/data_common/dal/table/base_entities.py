from sqlalchemy import Column, String, JSON
from data_common.dal.session_factory import Base


class BaseEntities(Base):
    __tablename__ = 'base_entities'

    entity_name = Column(String, primary_key=True)
    job_type = Column(String, primary_key=True)
    source = Column(String, primary_key=True)
    destination = Column(String)
    json_args = Column(JSON)


def __repr__(self):
        return "<ingestion_table(entity_name='%s',job_type='%s', source='%s', destination='%s', json_args='%s')>" % (
            self.entity_name, self.job_type, self.source,  self.destination, self.json_args)
