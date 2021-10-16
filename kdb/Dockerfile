FROM alpine:3.14.2
ENV QHOME /q
ENV PATH ${PATH}:${QHOME}/l32/

RUN apk update \
&& apk add --update binutils

# Download & unpack "libc6-i386 for amd64" debian package (this is required for "q" to run on a 64-bit OS).
# For information view: https://packages.debian.org/jessie/amd64/libc6-i386/
RUN LIBC32_DEB=libc6-i386_2.19-18+deb8u10_amd64.deb \
  &&  apk add dpkg \
  && dpkg --add-architecture i386 \ 
  && wget "http://ftp.us.debian.org/debian/pool/main/g/glibc/libc6-i386_2.19-18+deb8u10_amd64.deb" \
  && echo "aeee7bebb8e957e299c93c884aba4fa9  $LIBC32_DEB" | md5sum -c - \
  && ar p $LIBC32_DEB data.tar.xz | unxz | tar -x \
  && rm -rf $LIBC32_DEB /usr/share/doc/libc6-i386 /usr/lib32/gconv /usr/share/lintian \
	&& apk del binutils \
	&& rm -rf /var/lib/apk/lists/*
ARG DEBIAN_FRONTEND=noninteractive
#http://ftp.us.debian.org/debian/pool/main/g/glibc/libc6-i386_2.32-4_amd64.deb
COPY ./q32/q /q
COPY ./ticker /root/ticker
COPY ./tick.sh /root
RUN mkdir /data
RUN chmod 744 /root/tick.sh

## port for tickerplant
EXPOSE 5010
# port 5011 for rdb
EXPOSE 5011
#WORKDIR /root/ticker
#Start tickeplant
CMD ["sh", "/root/tick.sh"]