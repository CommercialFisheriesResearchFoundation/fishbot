FROM public.ecr.aws/lambda/python:3.13
WORKDIR ${LAMBDA_TASK_ROOT}


COPY lambda-fetch/requirements.txt .
COPY config/datasets.yaml .
RUN pip install -r requirements.txt

COPY utils/                  ./utils/
COPY botgrid/botgrid.gpkg    ./botgrid/botgrid.gpkg
COPY data/local_data/        ./data/local_data/
COPY data/bathymetry/       ./data/bathymetry/
# COPY data/statistical_areas/ ./data/statistical_areas/

COPY lambda-fetch/fishbot_data_pull.py .

CMD ["fishbot_data_pull.lambda_handler"]
