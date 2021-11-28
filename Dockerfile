FROM continuumio/miniconda3:latest

ARG VERSION=3.6

RUN pip install invoke

WORKDIR ploomber