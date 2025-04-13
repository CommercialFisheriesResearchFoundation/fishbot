FROM public.ecr.aws/lambda/python:3.10

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and directories
COPY main.py .
COPY utils ./utils
COPY botgrid ./botgrid
COPY data/local_data ./data/local_data
COPY data/bathymetry ./data/bathymetry
COPY data/statistical_areas ./data/statistical_areas

# Lambda handler
CMD ["main.lambda_handler"]
