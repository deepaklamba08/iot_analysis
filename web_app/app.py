import os

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


def __run_app(arguments: list):
    config = WebAppConfig(
        parameters={arguments[i]: arguments[i + 1] for i in range(0, len(arguments), 2)}
    )

    service.initialize(config=config)
    app.run(debug=True,
            host=config.get_value(key='host', default='127.0.0.1'),
            port=config.get_value(key='port', default=5000))


if __name__ == '__main__':
    p = ['app_config_file', 'E:\work\iot_analysis\\test\iot_analysis.json',
         'execution_summary_dir', 'E:\work\iot_analysis\\test\summary']
    __run_app(p)
