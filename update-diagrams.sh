#!/bin/bash
mvn clean package && cp target/generated-diagrams/Member-state-diagram.png src/docs
