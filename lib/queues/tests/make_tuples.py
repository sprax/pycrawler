import random
import URL
from PersistentQueue import define_record, RecordFIFO, b64
UrlParts = define_record("UrlParts", "scheme hostname port relurl")
f = RecordFIFO(UrlParts, (str, str, str, b64), "url_parts")

for line in random.sample(open("urls").readlines(), 100000):
    line = line.strip()
    try:
        parts = URL.get_parts(line)
    except URL.BadFormat, exc:
        print exc
        continue
    f.put(*parts)

f.close()
