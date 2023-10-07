import os
import sys

sys.path.append(os.environ['PATH_TO_WEB_APP'])

from flask import Flask, request, json
from flask import render_template

from web_app.service import WebAppService, WebAppConfig

app = Flask(__name__)
app.debug = True
app._static_folder = os.path.abspath("templates/static/")
service = WebAppService()


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


def __run_app(app_arguments: list):
    print(f'arguments - {app_arguments}')
    parameters = {app_arguments[i]: app_arguments[i + 1] for i in range(0, len(app_arguments), 2)}

    import yaml
    with open(parameters['config_file'], 'r') as stream:
        try:
            yaml_config = yaml.safe_load(stream)
            config = WebAppConfig(parameters=yaml_config['app'])
        except yaml.YAMLError as exc:
            raise Exception('error occurred while reading config file',exc)
    service.initialize(config=config)
    app.run(debug=True,
            host=config.get_value(key='host', default='127.0.0.1'),
            port=config.get_value(key='port', default=5000))


if __name__ == '__main__':
    arguments = sys.argv
    arguments = arguments[1:]
    #arguments = ['config_file', 'E:\work\iot_analysis\\test\webapp_config.yaml']
    __run_app(arguments)
