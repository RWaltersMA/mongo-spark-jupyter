# Choose your desired base image
FROM jupyter/pyspark-notebook:1a66dd36ff82

# built with the help of the below URL
# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#add-a-python-3-x-environment

# name your environment and choose python 3.x version
ARG conda_env=python37
ARG py_ver=3.7.7


#COPY --from=mambaorg/micromamba:0.17.0 "/home/${NB_USER}/mamba" "/home/${NB_USER}/mamba"
#RUN wget -qO- https://micromamba.snakepit.net/api/micromamba/linux-64/latest | tar -xvj bin/micromamba ./bin/micromamba shell init -s bash -p ~/micromamba
#RUN source ~/.bashrc

RUN conda --version
#RUN conda config --set allow_conda_downgrades true
#RUN conda install conda=4.6.141
RUN conda install -c conda-forge mamba

# you can add additional libraries you want mamba to install by listing them below the first line and ending with "&& \"
RUN mamba create --quiet --yes -p "${CONDA_DIR}/envs/${conda_env}" python=${py_ver} ipython ipykernel && \
    mamba clean --all -f -y

# alternatively, you can comment out the lines above and uncomment those below
# if you'd prefer to use a YAML file present in the docker build context

# COPY --chown=${NB_UID}:${NB_GID} environment.yml "/home/${NB_USER}/tmp/"
# RUN cd "/home/${NB_USER}/tmp/" && \
#     mamba env create -p "${CONDA_DIR}/envs/${conda_env}" -f environment.yml && \
#     mamba clean --all -f -y


# create Python 3.x environment and link it to jupyter
RUN "${CONDA_DIR}/envs/${conda_env}/bin/python" -m ipykernel install --user --name="${conda_env}" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# any additional pip installs can be added by uncommenting the following line
# RUN "${CONDA_DIR}/envs/${conda_env}/bin/pip" install

# prepend conda environment to path
ENV PATH "${CONDA_DIR}/envs/${conda_env}/bin:${PATH}"

# if you want this environment to be the default one, uncomment the following line:
# ENV CONDA_DEFAULT_ENV ${conda_env}