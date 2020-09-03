FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.0.0
ARG jupyterlab_version=2.2.6

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip3 install pyspark==${spark_version} jupyterlab==${jupyterlab_version} && \
    pip3 install wget && \
    pip3 install -U scikit-learn && \
    apt-get install -y python3-pandas && \
    pip3 install numpy && \
    pip3 install -U matplotlib && \ 
    pip3 install seaborn

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=