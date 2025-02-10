from logging.handlers import SMTPHandler
import smtplib
from email.message import EmailMessage
import email.utils
from typing import List, Optional
from pytarchive.service.log import logger


def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


def send_to_addr(header, content, emails: Optional[List[str]]):
    # We find out the SMTP handler and send the report to that email.
    try:
        handler: SMTPHandler = next(
            i for i in logger.handlers if isinstance(i, SMTPHandler)
        )
    except StopIteration:
        raise ValueError("No SMTP handler specified.")

    port = handler.mailport
    if not port:
        port = smtplib.SMTP_PORT
    smtp = smtplib.SMTP(handler.mailhost, port)
    msg = EmailMessage()
    msg["From"] = handler.fromaddr
    msg["To"] = ",".join(emails if emails is not None else handler.toaddrs)
    msg["Subject"] = header
    msg["Date"] = email.utils.localtime()
    msg.set_content(content)
    if handler.username:
        if handler.secure is not None:
            smtp.ehlo()
            smtp.starttls(*handler.secure)
            smtp.ehlo()
        smtp.login(handler.username, handler.password)
    smtp.send_message(msg)
    smtp.quit()


def send_to_logging_addr(header, content):
    return send_to_addr(header, content, None)
