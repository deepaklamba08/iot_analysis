import os
import sys

if 'PATH_TO_WEB_APP' in os.environ.keys():
    sys.path.append(os.environ['PATH_TO_WEB_APP'])

from flask import Flask, request, json
from flask import render_template
from web_app.service import WebAppService, WebAppConfig
import os


def load_config(config_file_path):
    import yaml
    with open(config_file_path, 'r') as stream:
        try:
            yaml_config = yaml.safe_load(stream)
            return yaml_config
        except yaml.YAMLError as exc:
            raise Exception('error occurred while reading config file', exc)


def create_app(arguments: list):
    app_arguments = {arguments[i]: arguments[i + 1] for i in range(0, len(arguments), 2)}
    yaml_config = load_config(app_arguments['config_file'])
    config = WebAppConfig(parameters=yaml_config['app'])
    app = Flask(config.get_value(key='app_name', default=__name__))

    service = WebAppService()
    service.initialize(config=config)
    app._static_folder = os.path.abspath(config.get_value(key='web_app_static_folder', default='static/'))
    app.debug = config.get_value(key='is_debug', default=True)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/jobs/all')
    def list_all_jobs():
        return service.list_all_jobs()

    @app.route('/jobs/run/', methods=['POST'])
    def run_job():
        content_type = request.headers.get('Content-Type')

        if content_type == 'application/json':
            request_data = json.loads(request.data.decode())
            return service.run_job(job_name=request_data['jobName'],
                                   job_parameters=json.loads(request_data['jobParameters']))
        else:
            return "Content type is not supported."

    @app.route('/jobs/<job_name>/app/details', methods=['GET'])
    def app_details(job_name: str):
        return service.application_details(job_name=job_name)

    @app.route('/jobs/<job_name>/details', methods=['GET'])
    def job_details(job_name: str):
        return service.job_details(job_name=job_name)

    @app.route('/jobs/history/<job_name>', methods=['GET'])
    def jobs_history(job_name: str):
        return service.jobs_history(job_name=job_name, is_current=False)

    @app.route('/jobs/status/<job_name>', methods=['GET'])
    def job_status(job_name: str):
        return service.jobs_history(job_name=job_name, is_current=True)

    return app
