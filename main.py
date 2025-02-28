#!penv/bin/python

import yadisk
import psycopg2
import os
import datetime

client = yadisk.Client(token="y0__xCe_rrQBxjUxTUgqrq3sBJl4wRAPK0drsWI7LTvi-OHV0FP9A")
print(client.check_token())

conn = psycopg2.connect(dbname="maps", host="localhost", user="zxc", password="a1", port="5432")
cursor = conn.cursor()

conn2 = psycopg2.connect(dbname="maps", host="localhost", user="zxc", password="a1", port="5432")
cursor2 = conn2.cursor()
conn2.autocommit = True


def get_content(url, type_s, id, name, folder):
    with client:
        try:
            anti = client.get_public_meta(url)["antivirus_status"]
            if anti == "clean":
                if client.get_public_meta(url)["type"] == 'file':
                    folder_path = folder + type_s + "/" + id
                    if not os.path.exists(folder_path):
                        os.mkdir(folder + type_s + "/" + id)
                    if client.get_public_meta(url)["media_type"] == "image":
                        path = folder_path + "/" + name + ".jpeg"
                        down_link = client.get_public_download_link(url)
                        client.download_by_link(down_link, path)
                        return path
                    elif client.get_public_meta(url)["media_type"] == "video":
                        path = folder_path + "/" + name + ".mov"
                        down_link = client.get_public_download_link(url)
                        client.download_by_link(down_link, path)
                        return path
                    else:
                        return ""
                else:
                    return ""
            else:
                return ""
        except:
             return ""

def fill_service():
    type_s = "service"
    cursor.execute("select id, photo_before, photo_left, photo_right, photo_front, video, photo_extra from service_log_data")
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
        
def fill_inspection():
    type_s = "inspection"
    cursor.execute("select id, photo_before, photo_left, photo_right, photo_front, video from inspection_log_data")
    for log in cursor.fetchall():
        id = str(log[0])
        photo_before = get_content(log[1], type_s, id, "photo_before", folder)
        photo_left = get_content(log[2], type_s, id, "photo_left", folder)
        photo_right = get_content(log[3], type_s, id, "photo_right", folder)
        photo_front = get_content(log[4], type_s, id, "photo_front", folder)
        video = get_content(log[5], type_s, id, "video", folder)
        cursor2.execute("update inspection_log_data set photo_before = %s, photo_left = %s, photo_right = %s, photo_front = %s, video = %s where id = %s",
        (photo_before, photo_left, photo_right, photo_front, video, id))        

folder = "test/media/"

def test():
    type_s = "inspection"
    cursor.execute("select id, photo_before, photo_left, photo_right, photo_front, video from inspection_log_data where id > 677 and id < 700")
    for log in cursor.fetchall():
        id = str(log[0])
        print(id)
        photo_before = get_content(log[1], type_s, id, "photo_before", folder)
        photo_left = get_content(log[2], type_s, id, "photo_left", folder)
        photo_right = get_content(log[3], type_s, id, "photo_right", folder)
        photo_front = get_content(log[4], type_s, id, "photo_front", folder)
        video = get_content(log[5], type_s, id, "video", folder)
        cursor2.execute("update inspection_log_data set photo_before = %s, photo_left = %s, photo_right = %s, photo_front = %s, video = %s where id = %s",
        (photo_before, photo_left, photo_right, photo_front, video, id))

start = datetime.datetime.now()
test()
end = datetime.datetime.now()
print(end - start)

cursor.close()
conn.close()

cursor2.close()
conn2.close()