FROM public.ecr.aws/lambda/python:3.13
WORKDIR ${LAMBDA_TASK_ROOT}

COPY lambda-process/requirements.txt .
RUN pip install -r requirements.txt

# Copy source code and directories
COPY utils/                  ./utils/
COPY botgrid/botgrid.gpkg    ./botgrid/botgrid.gpkg
# COPY data/local_data/        ./data/local_data/
# COPY data/bathymetry/        ./data/bathymetry/
# COPY data/statistical_areas/ ./data/statistical_areas/

COPY lambda-process/fishbot_data_process.py .
# Lambda handler
CMD ["fishbot_data_process.lambda_handler"]