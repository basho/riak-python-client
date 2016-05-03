import riak.pb.messages


def parse_pbuf_msg(msg_code, data):
    pbclass = riak.pb.messages.MESSAGE_CLASSES.get(msg_code, None)
    if pbclass is None:
        return None
    pbo = pbclass()
    pbo.ParseFromString(data)
    return pbo
