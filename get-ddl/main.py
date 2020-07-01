#! python
import commonfunc

cfg = './config'

conn = commonfunc.db_connect(cfg)
conn.test_connect()
