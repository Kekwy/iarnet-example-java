"""
帧解码 Python 实现，与 Main.java 中 VideoFrame -> DecodedFrame 约定一致。
运行时通过 functionIdentifier "decode_frame" 调用本模块的 decode_frame 函数。
"""


def decode_frame(input_obj: dict) -> dict:
    """
    输入: {"cameraId": str, "sequenceId": int, "rawBytes": str}
    输出: {"cameraId": str, "sequenceId": int, "decodedData": str}
    """
    return {
        "cameraId": input_obj["cameraId"],
        "sequenceId": input_obj["sequenceId"],
        "decodedData": input_obj["rawBytes"] + "_decoded",
    }

