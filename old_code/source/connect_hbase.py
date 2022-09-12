from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import THBaseService

transport = TTransport.TBufferedTransport(TSocket.TSocket('127.0.0.1', 9090))
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
client = THBaseService.Client(protocol)
transport.open()
# perform some operations with "client"
transport.close()
