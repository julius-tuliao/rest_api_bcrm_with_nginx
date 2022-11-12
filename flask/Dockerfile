# Step 1: Import the Os
from python:3.8-alpine

# Step 2: Install pip and Python Dependencies
RUN apk add --no-cache gcc libc-dev linux-headers
# Step 3: Create a Work Directory
WORKDIR /app

# Step 4: copy all Project Files
COPY . /app

# Step 5: Install all Dependencies for WSGI
RUN set -e; \
	apk add --no-cache --virtual .build-deps \
		gcc \
		libc-dev \
		linux-headers \
	; \
	pip --no-cache-dir install -r requirements.txt; \
	apk del .build-deps;

# Step 6:  run the command to start uWSGI
CMD ["uwsgi", "app.ini"]