#!penv/bin/python

import yadisk
import psycopg2
import datetime
from pathlib import Path

client = yadisk.Client(token="y0__xCe_rrQBxjUxTUgqrq3sBJl4wRAPK0drsWI7LTvi-OHV0FP9A")
print(client.check_token())

conn = psycopg2.connect(dbname="maps", host="localhost", user="zxc", password="sosat", port="5432")
cursor = conn.cursor()

conn2 = psycopg2.connect(dbname="maps", host="localhost", user="zxc", password="sosat", port="5432")
cursor2 = conn2.cursor()
conn2.autocommit = True


def get_content(id, url, media_type):
    path_file = folder + id + "." + media_type
    if Path(path_file).is_file():
        return True

    with client:
        try:
            meta = client.get_public_meta(url)
            anti = meta["antivirus_status"]
            type_file = meta["type"]
            down_link = client.get_public_download_link(url)
        except:
             return False

        if anti == "clean":
            if type_file == 'file':
                client.download_by_link(down_link, path_file)
                return True
            else:
                return False
        else:
            return False


def get_name(name):
    if name == "b":
        return "Фото до"
    elif name == "l":
        return "Фото слева"
    elif name == "r":
        return "Фото справа"
    elif name == "f":
        return "Фото спереди"
    elif name == "e":
        return "Дополнительное фото"
    elif name == "v":
        return "Видео"
    else:
        return ""


def fill_photo():
    cursor.execute("select id, media_name, media_type from media order by id")
    for log in cursor.fetchall():
        id = str(log[0])
        name = str(log[1])
        name_target = get_name(name[0])
        if name_target != "":
            name = name[1:]
        media_type = str(log[2])
        photo_exists = get_content(id, name, media_type)
        if photo_exists:
            cursor2.execute("update media set media_name = %s where id = %s", (name_target, id))
        else:
            cursor2.execute("delete from media where id = %s", (id))
        print(id, flush=True)
       

folder = "../gis-api/server/static/media/"


start = datetime.datetime.now()
print(start)

fill_photo()

end = datetime.datetime.now()
print(end)
print(end - start, flush=True)

cursor.close()
conn.close()

cursor2.close()
conn2.close()