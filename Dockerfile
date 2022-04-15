FROM python:3.10.4

WORKDIR /the/workdir/path

COPY config.txt ./

RUN pip install --no-cache-dir -r config.txt

COPY . .


CMD [ "python", './tgbot.py']