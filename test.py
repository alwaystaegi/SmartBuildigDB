from datetime import datetime

unixtime=1377275058

print((datetime.fromtimestamp(unixtime)).strftime('%Y-%m-%d %H:%M:%S'))