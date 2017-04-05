FROM scratch
LABEL maintainer Tom Taylor <tom.taylor@uswitch.com>

ADD ls-metrics /
ENTRYPOINT ["/ls-metrics"]
