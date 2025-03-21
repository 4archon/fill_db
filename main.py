#!penv/bin/python

import yadisk
import psycopg2
import os
import datetime
from pathlib import Path

client = yadisk.Client(token="y0__xCe_rrQBxjUxTUgqrq3sBJl4wRAPK0drsWI7LTvi-OHV0FP9A")
print(client.check_token())

conn = psycopg2.connect(dbname="maps", host="localhost", user="zxc", password="sosat", port="5432")
cursor = conn.cursor()

conn2 = psycopg2.connect(dbname="maps", host="localhost", user="zxc", password="sosat", port="5432")
cursor2 = conn2.cursor()
conn2.autocommit = True


def get_content(url, type_s, id, name, folder):
    folder_path = folder + type_s + "/" + id
    ref = "media/" + type_s + "/" + id
    path = folder_path + "/" + name
    href = ref + "/" + name
    path_img = path + ".jpeg"
    path_video = path + ".mov"
    href_img = href + ".jpeg"
    href_video = href + ".mov"
    if Path(path_img).is_file():
        return href_img
    if Path(path_video).is_file():
        return href_video

    with client:
        try:
            meta = client.get_public_meta(url)
            anti = meta["antivirus_status"]
            type_file = meta["type"]
            media = meta["media_type"]
            down_link = client.get_public_download_link(url)
        except:
             return ""

        if anti == "clean":
            if type_file == 'file':
                if not os.path.exists(folder_path):
                    os.mkdir(folder_path)
                if media == "image":
                    client.download_by_link(down_link, path_img)
                    return href_img
                elif media == "video":
                    client.download_by_link(down_link, path_video)
                    return href_video
                else:
                    return ""
            else:
                return ""
        else:
            return ""


def fill_service():
    type_s = "service"
    cursor.execute("select id, photo_before, photo_left, photo_right, photo_front, video, photo_extra from service_log_data order by id")
    for log in cursor.fetchall():
        id = str(log[0])
        photo_before = get_content(log[1], type_s, id, "photo_before", folder)
        photo_left = get_content(log[2], type_s, id, "photo_left", folder)
        photo_right = get_content(log[3], type_s, id, "photo_right", folder)
        photo_front = get_content(log[4], type_s, id, "photo_front", folder)
        photo_extra = get_content(log[6], type_s, id, "photo_extra", folder)
        video = get_content(log[5], type_s, id, "video", folder)
        cursor2.execute("update service_log_data set photo_before = %s, photo_left = %s, photo_right = %s, photo_front = %s, photo_extra = %s, video = %s where id = %s",
        (photo_before, photo_left, photo_right, photo_front, photo_extra, video, id))
        print(id + type_s, flush=True)
        
def fill_inspection():
    type_s = "inspection"
    cursor.execute("select id, photo_left, photo_right, photo_front, video from inspection_log_data order by id")
    for log in cursor.fetchall():
        id = str(log[0])
        photo_left = get_content(log[1], type_s, id, "photo_left", folder)
        photo_right = get_content(log[2], type_s, id, "photo_right", folder)
        photo_front = get_content(log[3], type_s, id, "photo_front", folder)
        video = get_content(log[4], type_s, id, "video", folder)
        cursor2.execute("update inspection_log_data set photo_left = %s, photo_right = %s, photo_front = %s, video = %s where id = %s",
        (photo_left, photo_right, photo_front, video, id))
        print(id + type_s, flush=True)        

folder = "../gis-api/server/static/media/"


start = datetime.datetime.now()
print(start)

fill_service()
fill_inspection()

end = datetime.datetime.now()
print(end)
print(end - start, flush=True)

cursor.close()
conn.close()

cursor2.close()
conn2.close()