# -*- coding: utf-8 -*-
import dingtalk.api
import json
import pymssql
import datetime
databse = 'test'
class connectDingTalk():
    def getAccessToken(self):
        req=dingtalk.api.OapiGettokenRequest("https://oapi.dingtalk.com/gettoken")
        req.appkey ="dingxcmroibxpsmybjm8"
        req.appsecret ="3G15m6PkqehY5LUdqSP7jkqPURVWa7yjsgY-OlIyRQ77FQD_3iMI7epqPnBMWJtA"
        global access_token
        access_token = req.getResponse().get("access_token")

        # # token 暂时写死
        # global access_token
        # access_token = "558ac6a27e733baa9ab2db9e0bde2294"
        # # token 暂时写死

        print(access_token)
        return access_token
    # 获取在职员工的userid列表
    def getOnjobUserlist(self):
        req=dingtalk.api.OapiSmartworkHrmEmployeeQueryonjobRequest("https://oapi.dingtalk.com/topapi/smartwork/hrm/employee/queryonjob")
        req.status_list = "2,3"
        req.offset = 0
        req.size = 40
        userlist = []
        global onjobUserlist #此处生命全局 为了查打卡结果
        onjobUserlist = []
        try:
            resp = req.getResponse(access_token)
            print("成功获取响应-花名册json：")
            # print(resp)
            userlist.append(resp.get("result").get("data_list"))
            # resplist = resp.get("result").get("data_list")
            while ("next_cursor" in resp['result']):
                req.offset = req.offset+req.size
                resp = req.getResponse(access_token)
                # resplist.append(resp.get("result").get("data_list"))
                userlist.append(resp.get("result").get("data_list"))
            # print(resplist)
            # print(len(resplist))
            # for x in range(len(userlist)):
            #     print(userlist[x])
            #     connectDingTalk.getCheckinfo(self,userlist[x])
            for i in range(len(userlist)):
                for j in range(len(userlist[i])):
                    onjobUserlist.append(userlist[i][j])
            print("转换出onjobUserlist:")
            print(len(onjobUserlist))
            # print(onjobUserlist)
            return onjobUserlist
        except Exception as e:
            print(e)

    def getDeplist(self):
        req=dingtalk.api.OapiDepartmentListRequest("https://oapi.dingtalk.com/department/list")
        try:
            resp = req.getResponse(access_token)
            print("成功获取响应-部门json：")
            print(resp)
            depjson = resp.get("department")
            global deplist
            deplist = []
            for i in depjson[1:]:
                depinfo = {'depid':i.get("id"),'depname':i.get("name")}
                deplist.append(depinfo)
            print("部门info处理完成:")
            print(deplist)
            return deplist
        except Exception as e:
            print(e)
    def getUserInfo(self,depid):
        req=dingtalk.api.OapiV2UserListRequest("https://oapi.dingtalk.com/topapi/v2/user/list")
        req.dept_id = depid
        req.cursor = 0
        req.size = 50
        try:
            resp = req.getResponse(access_token)
            all_resp = []
            all_resp.append(resp.get("result").get("list"))
            while (resp['result']['has_more'] == True):
                req.cursor = req.cursor+req.size
                resp = req.getResponse(access_token)
                all_resp.append(resp.get("result").get("list"))
            return all_resp
        except Exception as e:
            print(e)

    def getUserInfoFromDeplist(self):
        # userinfo(dict)
        allUserInfo = {}
        #userinfo(data)
        global userinfodata
        userinfodata = []
        # 先获取json
        for i in range(len(deplist)):
            allUserInfo[i] = connectDingTalk.getUserInfo(self,deplist[i]['depid'])
        print(len(allUserInfo))
        # 整理数据为python字典
        for j in range(len(allUserInfo)):
            for k in range(len(allUserInfo[j])):
                for l in range(len(allUserInfo[j][k])):
                    info = {'name':allUserInfo[j][k][l]['name'],'userid':allUserInfo[j][k][l]['userid'],'depid':allUserInfo[j][k][l]['dept_id_list'][0]}
                    if info not in userinfodata:
                        userinfodata.append(info)
        print(userinfodata)
        print(len(userinfodata))

    def getCheckinfo(self):
        checkDateFrom = datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d 00:00:00")
        checkDateTo = datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d 23:59:59")
        # 考勤列表暂时写死，需要从userlist获取
        checkUserIdList = ["022935515930625665","186830624439954676"]
        # checkUserIdList = onjobUserlist
        req=dingtalk.api.OapiAttendanceListRequest("https://oapi.dingtalk.com/attendance/list")
        req.workDateFrom = checkDateFrom
        req.workDateTo = checkDateTo
        req.userIdList = checkUserIdList
        req.offset = 0
        req.limit = 2
        recordresult = []
        checkResultInfo=[]
        try:
            resp = req.getResponse(access_token)
            recordresult.append(resp.get("recordresult"))
            while (resp['hasMore']==True):
                req.offset = req.offset+req.limit
                resp = req.getResponse(access_token)
                recordresult.append(resp.get("recordresult"))

            for i in range(len(recordresult)):
                for j in range(len(recordresult[i])):
                    checkResultInfo.append(recordresult[i][j])
            print(checkResultInfo)
            print(len(checkResultInfo))
            connectDingTalk.updateCheckInfoToDB(self,checkResultInfo)
            return checkResultInfo
        except Exception as e:
            print(e)
    def updateCheckInfoToDB(self,checkResultInfo):
        connect = pymssql.connect('116.62.6.62', 'sa', 'sa123456', ''+databse+'')
        cursor = connect.cursor()
        try:
            for i in range(len(checkResultInfo)):
                # 先拿到所有key转化为str
                dbkeys = ','.join(list(checkResultInfo[i].keys()))
                # 先拿到所有value转为str
                dbvalueslist = [str(x) for x in list(checkResultInfo[i].values())]
                # 给所有value加''作为字符串
                dbvalues = ','.join("'"+ item +"'" for item in dbvalueslist)
                sql = "insert into tb_checkInfo ("+dbkeys+") values ("+dbvalues+")"
                cursor.execute(sql)
                connect.commit()
            cursor.close()
            connect.close()
            print("insert success")
        except Exception as e:
            print(e)

    # updateUsers() 更新dingtalk的在职员工信息 → SQLserver，查找userid有则update，无则insert
    def updateUsers(self):
        connect = pymssql.connect('116.62.6.62', 'sa', 'sa123456', ''+databse+'')
        cursor = connect.cursor()
        try:
            for i in range(len(userinfodata)):
                sql ="IF Exists(select userid from tb_users where userid='%s') update tb_users set name='%s', depid = '%s', employstatus='true' where userid='%s' ELSE insert into tb_users (userid,name,depid,employstatus) values('%s','%s','%s','true')"%(userinfodata[i]['userid'],userinfodata[i]['name'],userinfodata[i]['depid'],userinfodata[i]['userid'],userinfodata[i]['userid'],userinfodata[i]['name'],userinfodata[i]['depid'])
                cursor.execute(sql)
                connect.commit()
            print("插入数据库成功")
            # 更新员工在职状态，更改不在onjobUserlist中的员工为离职状态。
            sql_updateEmploystatus = "update tb_users set employstatus = 0  where userid in (select userid from tb_users where userid not in (select userid from tb_onjobUsers))"
            cursor.execute(sql_updateEmploystatus)
            connect.commit()
            print("更新在职状态成功")
            cursor.close()
            connect.close()
        except Exception as e:
            print(e)
    def replaceDepinfo(self):
        connect = pymssql.connect('116.62.6.62', 'sa', 'sa123456', ''+databse+'')
        cursor = connect.cursor()
        try:
            sql_drop = "TRUNCATE TABLE tb_departments"
            cursor.execute(sql_drop)
            connect.commit()
            for i in range(len(deplist)):
                sql = "insert into tb_departments (depid,depname) values ('%s','%s')"%(deplist[i]['depid'],deplist[i]['depname'])
                cursor.execute(sql)
                connect.commit()
            print("replace替换部门表成功")
            cursor.close()
            connect.close()
        except Exception as e:
            print(e)
    def replaceOnjobUsers(self):
        onjobUserlist = connectDingTalk.getOnjobUserlist(self)
        connect = pymssql.connect('116.62.6.62', 'sa', 'sa123456', ''+databse+'')
        cursor = connect.cursor()
        try:
            sql_drop = "TRUNCATE TABLE tb_onjobUsers"
            cursor.execute(sql_drop)
            connect.commit()
            for i in range(len(onjobUserlist)):
                sql = "insert into tb_onjobUsers (userid) values ('%s')"%(onjobUserlist[i])
                cursor.execute(sql)
                connect.commit()
            print("replace替换在职员工表成功")
            cursor.close()
            connect.close()
        except Exception as e:
            print(e)

run = connectDingTalk()
run.getAccessToken()
run.getDeplist() #获取部门列表
run.replaceOnjobUsers() #先更新在职员工表 tb_onjobUsers
run.getUserInfoFromDeplist() #再通过部门查询在职员工info
run.replaceDepinfo() #更新部门表 tb_departments
run.updateUsers() #更新用户表 tb_users（包括离职员工）
run.getCheckinfo() #获取考勤信息API 并调用updateCheckInfoToDB插入数据库
# run.updateCheckInfoToDB() #更新打卡记录表 在查在职员工表时 先查考勤，然后调用此函数  插入数据库
