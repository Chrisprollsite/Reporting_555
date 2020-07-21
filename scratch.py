from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

postgre_port = 5432
postgre_ip = "localhost"
postgre_db_name = "555"
postgre_user = "postgres"
postgre_pwd = "171286"
cmd_connection = "postgresql://%s:%s@%s:%i/%s" % (postgre_user, postgre_pwd, postgre_ip, postgre_port, postgre_db_name)

# #postgre_db=sqlalchemy.create_engine('postgresql://postgres:171286@localhost:5432/BentleyNetwork')
# postgre_db = psycopg2.connect(host="localhost",database="555", user="postgres", password="171286")
# psycopg2.connect(cmd_connection)

db_engine = create_engine(cmd_connection,echo=True)

meta = MetaData()

students = Table(
   'students', meta,
   Column('id', Integer, primary_key = True),
   Column('name', String),
   Column('lastname', String),
)
meta.create_all(db_engine)

db_engine.dialect.has_table(db_engine, 'studesdfnts')