from enum import IntEnum


class GInsWSWorkerTypes(IntEnum):
    WSWorkerType_NotSpecified = 0
    WSWorkerType_XmlRpcCall = 1
    WSWorkerType_OnlineData = 2
    WSWorkerType_Authentication = 13
    WSWorkerType_SystemState = 15
    WSWorkerType_MessageEvents = 17


class GInsWSMessageTypes(IntEnum):
    WSMsgType_NotSpecified = 0
    WSMsgType_Subscribe = 1
    WSMsgType_Publish = 2
    WSMsgType_Stop = 3
    WSMsgType_Start = 4
    WSMsgType_Reconfigure = 5
    WSMsgType_Destroy = 6
    WSMsgType_Request = 7
    WSMsgType_Response = 8
    WSMsgType_Identify = 9
    WSMsgType_Error = 10
    WSMsgType_End = 11
    WSMsgType_NotRoutable = 12
    WSMsgType_Authentication = 13


class GInsWSWorkerMessageFormat(IntEnum):
    WSMsgFormat_JSON = 0
    WSMsgFormat_BINARY = 1
    WSMsgFormat_STRING = 2
    WSMsgFormat_UNKNOWN = 3
