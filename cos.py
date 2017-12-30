# -*- coding: utf-8 -*-

import os
import time
import random
import hmac
import hashlib
import base64
import binascii
import urllib.request
import urllib.parse
import requests


class CosConfig(object):
    app_id = 0
    secret_id = ''
    secret_key = ''
    bucket = ''
    region = ''


class CosAuth(object):
    def __init__(self, config):
        self.config = config

    def app_sign(self, bucket, cos_path, expired, upload_sign=True):
        app_id = self.config.app_id
        cos_path = urllib.parse.quote(cos_path, '~/')
        if upload_sign:
            fileid = '/%s/%s/%s' % (app_id, bucket, cos_path)
        else:
            fileid = cos_path
        now = int(time.time())
        if expired != 0 and expired < now:
            expired += now
        rdm = random.randint(0, 999999999)
        original = 'a=%s&k=%s&e=%d&t=%d&r=%d&f=%s&b=%s' % (
            app_id, self.config.secret_id, expired, now, rdm, fileid, bucket)
        original = original.encode('u8')
        hmac_sha1 = hmac.new(self.config.secret_key.encode('u8'), original, hashlib.sha1)
        sign_tmp = hmac_sha1.hexdigest()
        sign_tmp = binascii.unhexlify(sign_tmp)
        sign_base64 = base64.b64encode(sign_tmp + original)

        return sign_base64.decode('u8')

    def sign_once(self, bucket, cos_path):
        """单次签名(针对删除和更新操作)
        :param bucket: bucket名称
        :param cos_path: 要操作的cos路径, 以'/'开始
        :return: 签名字符串
        """
        return self.app_sign(bucket, cos_path, 0)

    def sign_more(self, bucket, cos_path, expired):
        """多次签名(针对上传文件，创建目录, 获取文件目录属性, 拉取目录列表)
        :param bucket: bucket名称
        :param cos_path: 要操作的cos路径, 以'/'开始
        :param expired: 签名过期时间, UNIX时间戳, 如想让签名在30秒后过期, 即可将expired设成当前时间加上30秒
        :return: 签名字符串
        """
        return self.app_sign(bucket, cos_path, expired)

    def sign_download(self, bucket, cos_path, expired):
        """下载签名(用于获取后拼接成下载链接，下载私有bucket的文件)
        :param bucket: bucket名称
        :param cos_path: 要下载的cos文件路径, 以'/'开始
        :param expired:  签名过期时间, UNIX时间戳, 如想让签名在30秒后过期, 即可将expired设成当前时间加上30秒
        :return: 签名字符串
        """
        return self.app_sign(bucket, cos_path, expired, False)


class CosOp(object):
    def __init__(self, cos_config, bucket_name):
        """初始化操作
        """
        self.config = cos_config
        self.config.bucket = bucket_name
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/63.0.3239.108 Safari/537.36'}
        self.host = "http://{region}.file.myqcloud.com".format(region=self.config.region)
        self.base_url = "http://{region}.file.myqcloud.com/files/v2/{appid}/{bucket}/".format(
            region=self.config.region, appid=self.config.app_id, bucket=self.config.bucket)

    def create_folder(self, dir_name):
        """创建目录(https://cloud.tencent.com/document/product/436/6061)
        :param dir_name:要创建的目录的目录的名称
        :return 返回True创建成功，返回False创建失败
        """
        dir_name = dir_name.lstrip('/')
        url = "{base_url}{dir_name}/".format(base_url=self.base_url, dir_name=dir_name)

        data = {"op": "create", "biz_attr": ""}

        self.headers['Authorization'] = CosAuth(self.config).sign_more(self.config.bucket, '', 30)
        resp = requests.post(url, json=data, headers=self.headers)
        resp = resp.json()

        if resp.get("code") == 0:
            return True
        return False

    def list_folder(self, dir_name=None, prefix=None, num=199, context=None):
        """列目录(https://cloud.tencent.com/document/product/436/6062)
        :param dir_name:文件夹名称
        :param prefix:前缀
        :param num:查询的文件的数量，最大支持1000，默认查询数量为199
        :param context:翻页标志，将上次查询结果的context的字段传入，即可实现翻页的功能
        :return 查询结果，为dict格式
        """
        url = self.base_url
        if dir_name is not None:
            dir_name = dir_name.lstrip('/')
            url = url + dir_name + "/"
        if prefix is not None:
            url = url + prefix
        url = url + "?op=list&num=" + str(num)
        if context is not None:
            url = url + '&context=' + context

        self.headers['Authorization'] = CosAuth(self.config).sign_more(self.config.bucket, '', 30)
        resp = requests.get(url, headers=self.headers).json()

        return resp

    def query_folder(self, dir_name):
        """查询目录属性(https://cloud.tencent.com/document/product/436/6063)
        :param dir_name:查询的目录的名称
        :return:查询出来的结果，为dict格式
        """

        dir_name = dir_name.lstrip('/')
        url = "{base_url}{dir_name}/?op=stat".format(base_url=self.base_url, dir_name=dir_name)

        self.headers['Authorization'] = CosAuth(self.config).sign_more(self.config.bucket, '', 30)
        resp = requests.get(url, headers=self.headers).json()

        return resp

    def delete_folder(self, dir_name):
        """删除目录(https://cloud.tencent.com/document/product/436/6064)
        :param dir_name:删除的目录的目录名
        :return: 删除结果，成功返回True，失败返回False
        """
        dir_name = dir_name.lstrip('/')
        url = "{base_url}{dir_name}/".format(base_url=self.base_url, dir_name=dir_name)

        data = {"op": "delete"}

        self.headers['Authorization'] = CosAuth(self.config).sign_once(self.config.bucket, dir_name + '/')
        resp = requests.post(url, json=data, headers=self.headers).json()

        if resp.get('code') == 0:
            return True
        return False

    def upload_file(self, local_path, file_name, dir_name=None):
        """简单上传文件(https://cloud.tencent.com/document/product/436/6066)
        :param local_path: 文件的物理地址
        :param file_name: 文件名称
        :param dir_name: 文件夹名称（可选）
        :return: {'access_url': 'cdn link', 'source_url': 'src link',
                  'url': 'operation link', 'resource_path': 'src path'}
        """
        dir_name = dir_name.lstrip('/') if dir_name else ""
        url = self.base_url
        if dir_name is not None:
            url = url + dir_name + '/' + file_name

        headers = {'Authorization': CosAuth(self.config).sign_more(self.config.bucket, '', 30)}
        resp = requests.post(
            url=url, data={'op': 'upload', 'biz_attr': '', 'insertOnly': '0'}, headers=headers,
            files={'filecontent': (local_path, open(local_path, 'rb'), 'application/octet-stream')}).json()

        return resp.get('data')

    def _upload_slice_control(self, url, file_size, slice_size):
        """
        :param file_size: 文件大小 单位为 Byte
        :param slice_size: 分片大小 单位为 Byte 有效值：1048576 (1 MB)
        :return:
        """
        data = {'op': 'upload_slice_init', 'filesize': str(file_size),
                'slice_size': str(slice_size), 'biz_attr': '', 'insertOnly': '0'}

        headers = {'Authorization': CosAuth(self.config).sign_more(self.config.bucket, '', 30)}
        resp = requests.post(url=url, files=data, headers=headers).json()

        return resp.get('data', {}).get('session')

    def _upload_slice_data(self, url, filecontent, session, offset):
        data = {'op': 'upload_slice_data', 'filecontent': filecontent,
                'session': session, 'offset': str(offset)}

        headers = {'Authorization': CosAuth(self.config).sign_more(self.config.bucket, '', 30)}
        resp = requests.post(url=url, files=data, headers=headers).json()

        return resp.get('data')

    def _upload_slice_finish(self, url, session, file_size):
        data = {'op': 'upload_slice_finish', 'session': session, 'filesize': str(file_size)}

        headers = {'Authorization': CosAuth(self.config).sign_more(self.config.bucket, '', 30)}
        resp = requests.post(url=url, files=data, headers=headers).json()

        return resp.get('data')

    def upload_slice_file(self, real_file_path, file_name, dir_name=None, slice_size=1048576, offset=0):
        """
         文件分片上传 20 MB 以上
        :param real_file_path: 文件的物理地址
        :param slice_size: 分片大小 单位为 Byte 有效值：1048576 (1 MB)
        :param file_name: 文件名称
        :param offset: 本次分片偏移量
        :param dir_name: 文件夹名（可选）
        :return:
        """
        dir_name = dir_name.lstrip('/') if dir_name else ""
        url = self.base_url
        if dir_name is not None:
            url = url + dir_name + '/' + file_name

        file_size = os.path.getsize(real_file_path)
        session = self._upload_slice_control(url, file_size=file_size, slice_size=slice_size)

        with open(real_file_path, 'rb') as local_file:
            while offset < file_size:
                file_content = local_file.read(slice_size)
                self._upload_slice_data(url, filecontent=file_content, session=session, offset=offset)
                offset += slice_size
            resp = self._upload_slice_finish(url, session=session, file_size=file_size)

        return resp

    def upload_file_from_url(self, url, file_name, dir_name=None):
        """从url上传文件
        :param url: 文件url地址
        :param file_name: 文件名称
        :param dir_name: 文件夹名称（可选）
        :return: 文件url dict
        """
        real_file_name = str(int(time.time() * 1000)) + str(random.randint(0, 9999))
        urllib.request.urlretrieve(url, real_file_name)

        data = self.upload_file(real_file_name, file_name, dir_name)
        os.remove(real_file_name)

        return data

    def move_file(self, source_fileid, dest_fileid):
        """
        :param source_fileid: 源文件
        :param dest_fileid: 目标文件, 以'/'开头从bucket下开始查找，否则从源文件所在目录开始查找
        :return: 成功返回True，失败返回False
        """
        source_fileid = source_fileid.replace("\\", "/").strip('/')
        dest_fileid = dest_fileid.replace("\\", "/").rstrip('/')
        url = "{base_url}{source_fileid}".format(base_url=self.base_url, source_fileid=source_fileid)

        headers = {'Authorization': CosAuth(self.config).sign_once(self.config.bucket, source_fileid)}
        resp = requests.post(
            url=url, data={'op': 'move', 'dest_fileid': dest_fileid, 'to_over_write': '0'},
            files={'filecontent': ('', '', 'application/octet-stream')}, headers=headers).json()

        if resp.get('code') == 0:
            return True
        return False

    def copy_file(self, source_fileid, dest_fileid):
        """
        :param source_fileid: 源文件
        :param dest_fileid: 目标文件, 以'/'开头从bucket下开始查找，否则从源文件所在目录开始查找
        :return: 成功返回True，失败返回False
        """
        source_fileid = source_fileid.replace("\\", '/').strip('/')
        dest_fileid = dest_fileid.replace("\\", "/").rstrip('/')
        url = "{base_url}{source_fileid}".format(base_url=self.base_url, source_fileid=source_fileid)

        headers = {'Authorization': CosAuth(self.config).sign_once(self.config.bucket, source_fileid)}
        resp = requests.post(
            url=url, data={'op': 'copy', 'dest_fileid': dest_fileid, 'to_over_write': '0'},
            files={'filecontent': ('', '', 'application/octet-stream')}, headers=headers).json()

        if resp.get('code') == 0:
            return True
        return False

    def delete_file(self, dest_fileid):
        """
        :param dest_fileid: 目标文件
        :return: 成功返回True，失败返回False
        """
        dest_fileid = dest_fileid.replace("\\", "/").strip('/')
        url = "{base_url}{dest_fileid}".format(base_url=self.base_url, dest_fileid=dest_fileid)

        data = {"op": "delete"}

        self.headers['Authorization'] = CosAuth(self.config).sign_once(self.config.bucket, dest_fileid)
        resp = requests.post(url, json=data, headers=self.headers).json()

        if resp.get('code') == 0:
            return True
        return False


class Cos(object):
    def __init__(self, app_id, secret_id, secret_key, region="sh"):
        self.config = CosConfig()
        self.config.app_id = int(app_id)
        self.config.secret_id = secret_id
        self.config.secret_key = secret_key
        self.config.region = region

    def get_bucket(self, bucket_name):
        return CosOp(self.config, bucket_name)


if __name__ == '__main__':
    cos = Cos(app_id=0, secret_id='', secret_key='', region='sh')
    bk = cos.get_bucket("test01")

    # print('step1', bk.create_folder('cos_test'))

    # print('step2', bk.list_folder())

    # print('step3', bk.create_folder('cos_test_more'))

    # print('step4', bk.list_folder(num=1, context='/test01/cos_test/'))

    # print('step5', bk.query_folder('cos_test'))

    # print('step6', bk.delete_folder('cos_test_more'))

    # print('step7', bk.upload_file('/code/cos-py3/README.md', 'readme01.md'))

    # print('step8', bk.upload_file('/code/cos-py3/README.md', 'readme02.md', dir_name='cos_test'))

    # print('step9', bk.upload_slice_file('/code/cos-py3/README.md', 'readme_slice01.md'))

    # print('step10', bk.upload_slice_file('/code/cos-py3/README.md', 'readme_slice02.md', 'cos_test'))

    # print('step11', bk.upload_file_from_url('http://xxx.jpg', 'avatar.jpg', dir_name='cos_test'))

    # print('step12', bk.move_file('cos_test/readme02.md', 'readme03.md'))

    # print('step13', bk.copy_file('readme01.md', '/cos_test/readme02.md'))

    # print('step14', bk.delete_file('readme01.md'))

    # print('step15', bk.delete_file('readme_slice01.md'))

    # print('step16', bk.delete_file('cos_test/readme02.md'))

    # print('step17', bk.delete_file('cos_test/readme03.md'))

    # print('step18', bk.delete_file('cos_test/readme_slice02.md'))

    # print('step19', bk.delete_file('cos_test/avatar.jpg'))

    # print('step20', bk.delete_folder('cos_test'))
