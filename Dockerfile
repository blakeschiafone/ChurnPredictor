FROM python:3

LABEL maintainer="rstudio"

RUN apt-get -y update \
  && apt-get install -y git-core \
  libnlopt-dev \
	libcurl4-openssl-dev \
	libssl-dev \
	libjpeg-dev \
	make \
	pandoc \
	pandoc-citeproc \
	curl \
	python3-pip

RUN ldconfig -v

# Install Python packages
RUN pip3 install google-cloud-datastore

# Downloading gcloud package
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

# Now install R and littler, and create a link for littler in /usr/local/bin
RUN apt-get --force-yes update \
    && apt-get --assume-yes install r-base-core

RUN Rscript -e "install.packages('reticulate', repos='http://cran.us.r-project.org/')"

RUN Rscript -e "install.packages(c('nlme', 'bigrquery', 'bit64', 'caret', 'devtools', 'dplyr', 'FFTrees', 'glue', 'plumber', 'plyr', 'purrr', 'R6', 'readr', 'remotes', 'reticulate', 'Rcpp', 'tibble', 'tidyr'), repos='http://cran.us.r-project.org/', Ncpus = 6)"	

RUN adduser --disabled-password --gecos '' admin \
    && adduser admin sudo \
    && echo '%sudo ALL=(ALL:ALL) ALL' >> /etc/sudoers
    
WORKDIR /churnpredictor
COPY [".", "./"]
RUN chown -R admin:admin /churnpredictor
RUN chmod 755 /churnpredictor
USER admin

# Set credentials
RUN gcloud auth activate-service-account --key-file=tndevel_auth.json
RUN gcloud config set project tn-devel

EXPOSE 8080
ENTRYPOINT ["R", "-e", "pr <- plumber::plumb('main.R'); pr$run(host='0.0.0.0', port=8080)"]