FROM spark-base

# -- Layer: JupyterLab

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
