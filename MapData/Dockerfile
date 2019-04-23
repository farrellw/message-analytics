FROM hseeberger/scala-sbt

RUN mkdir build
COPY . /build
WORKDIR /build

RUN sbt test