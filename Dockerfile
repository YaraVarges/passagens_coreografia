FROM buildpack-deps:stretch

ENV PATH /usr/local/bin:$PATH
ENV LANG C.UTF-8

RUN apt-get update && apt dist-upgrade -y && apt-get install -y --no-install-recommends \
		tk-dev \
		uuid-dev \
	&& rm -rf /var/lib/apt/lists/*

ENV PYTHON_VERSION 3.7.3
ENV PYTHON_FILE 3.7.3
RUN set -ex \
	\
	&& wget -O python.tar.xz "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_FILE.tar.xz" \
	&& wget -O python.tar.xz.asc "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_FILE.tar.xz.asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& mkdir -p /usr/src/python \
	&& tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz \
	&& rm python.tar.xz \
	\
	&& cd /usr/src/python \
	&& gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)" \
	&& ./configure \
		--build="$gnuArch" \
		--enable-loadable-sqlite-extensions \
		--enable-shared \
		--with-system-expat \
		--with-system-ffi \
		--without-ensurepip \
	&& make -j "$(nproc)" \
	&& make install \
	&& ldconfig \
	\
	&& find /usr/local -depth \
		\( \
			\( -type d -a \( -name test -o -name tests \) \) \
			-o \
			\( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
		\) -exec rm -rf '{}' + \
	&& rm -rf /usr/src/python

ENV PYTHON_PIP_VERSION 19.1.1
RUN set -ex; \
	\
	wget -O get-pip.py 'https://bootstrap.pypa.io/get-pip.py'; \
	\
	python3 get-pip.py \
		--disable-pip-version-check \
		--no-cache-dir \
		"pip==$PYTHON_PIP_VERSION" \
	; \
	pip --version; \
	\
	find /usr/local -depth \
		\( \
			\( -type d -a \( -name test -o -name tests \) \) \
			-o \
			\( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
		\) -exec rm -rf '{}' +; \
	rm -f get-pip.py

RUN pip install Flask
RUN pip install faker
RUN pip install kafka-python
RUN pip install flask_apscheduler

RUN mkdir /workdir
WORKDIR /workdir

RUN adduser --disabled-password --force-badname --shell /bin/bash --gecos "User" ri6qbuRYvtXfHM9L 
RUN rm -rf /home/ri6qbuRYvtXfHM9L
USER ri6qbuRYvtXfHM9L

EXPOSE 5000
