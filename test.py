import dingtalk.api
import datetime
import pymssql
access_token = "4e0d5dc695af3e0cb181b0312526f5fb"
databse='test'
class connectDingTalk():
    def getOnjobUserlist(self):
            req=dingtalk.api.OapiSmartworkHrmEmployeeQueryonjobRequest("https://oapi.dingtalk.com/topapi/smartwork/hrm/employee/queryonjob")
            req.status_list = "2,3"
            req.offset = 0
            req.size = 20
            userlist = []
            global onjobUserlist #此处生命全局 为了查打卡结果
            onjobUserlist = []
            try:
                resp = req.getResponse(access_token)
                print("成功获取响应-花名册json：")
                # print(resp)
                userlist.append(resp.get("result").get("data_list"))
                resplist = resp.get("result").get("data_list")
                while ("next_cursor" in resp['result']):
                    req.offset = req.offset+req.size
                    resp = req.getResponse(access_token)
                    resplist.append(resp.get("result").get("data_list"))
                    userlist.append(resp.get("result").get("data_list"))
                print(resplist)
                print(len(resplist))
                for x in range(len(userlist)):
                    print(userlist[x])
                    connectDingTalk.getCheckinfo(self,userlist[x])
                for i in range(len(userlist)):
                    for j in range(len(userlist[i])):
                        onjobUserlist.append(userlist[i][j])
                print("转换出onjobUserlist:")
                print(len(onjobUserlist))
                # print(onjobUserlist)
                return onjobUserlist
            except Exception as e:
                print(e)
    def getCheckinfo(self,userlist):
            checkDateFrom = datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d 00:00:00")
            checkDateTo = datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d 23:59:59")
            # 考勤列表暂时写死，需要从userlist获取
            # checkUserIdList = ["022935515930625665","186830624439954676"]
            checkUserIdList = userlist
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
run = connectDingTalk()
run.getOnjobUserlist()
