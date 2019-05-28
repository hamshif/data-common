from sqlalchemy import Column, Integer, String, Boolean, JSON
from data_common.dal.session_factory import Base


class AccountConfig(Base):
    __tablename__ = 'account_config'

    namespace_id = Column(Integer, primary_key=True)
    source = Column(String, primary_key=True)
    entity = Column(String, primary_key=True)
    destination = Column(String)
    job_type = Column(String)
    json_args = Column(JSON)
    is_active = Column(Boolean)

    def __repr__(self):
            return "<AccountConfig(namespace_id='%s', source='%s', entity='%s', destination='%s', job_type='%s', json_args='%s', is_active='%s')>" % (
                                 self.namespace_id, self.source, self.entity, self.destination, self.job_type, self.json_args, self.is_active)
