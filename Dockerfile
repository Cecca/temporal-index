FROM rust:1.44.1
 ARG USER_NAME
 ARG USER_ID
 ARG GROUP_ID
# We install some useful packages
#  RUN apt-get update -qq
#  RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata
#  RUN apt-get install -y vim valgrind python wget git linux-tools-generic build-essential ghc
 RUN addgroup --gid $GROUP_ID user; exit 0
 RUN adduser --disabled-password --gecos '' --uid $USER_ID --gid $GROUP_ID $USER_NAME; exit 0
 RUN echo 'root:Docker!' | chpasswd
 ENV TERM xterm-256color
 USER $USER_NAME

