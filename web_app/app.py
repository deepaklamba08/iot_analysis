import os
import sys

if 'PATH_TO_WEB_APP' in os.environ.keys():
    sys.path.append(os.environ['PATH_TO_WEB_APP'])

from flask import Flask, request, json, redirect, url_for, session, flash
from flask import render_template
from web_app.service import WebAppService, WebAppConfig
from src.utils import read_config_file, get_env_config
import os
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash


def create_app(arguments: list):
    app_arguments = {arguments[i]: arguments[i + 1] for i in range(0, len(arguments), 2)}
    yaml_config = read_config_file(app_arguments['config_file'])
    config = WebAppConfig(parameters=yaml_config['app'])

    template_folder = config.get_property('template_folder')
    app = Flask(config.get_value(key='app_name', default=__name__), template_folder=template_folder)

    service = WebAppService(config)
    app._static_folder = os.path.abspath(config.get_value(key='web_app_static_folder', default='static/'))
    app.debug = config.get_value(key='is_debug', default=True)
    app.secret_key = get_env_config(config.get_property('api_secret_key'))

    def login_required(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'username' not in session:
                return redirect(url_for('login'))
            return f(*args, **kwargs)

        return decorated_function

    @app.route('/')
    def home():
        return redirect(url_for('login'))

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if request.method == 'POST':
            username = request.form['username']
            password = request.form['password']
            user_data = service.get_user(login=username)
            if not user_data:
                flash('User does not exist', 'danger')
            elif not user_data.status:
                flash('user is inactive', 'danger')
            elif check_password_hash(user_data.password, password):
                session['username'] = username
                return redirect(url_for('index'))
            else:
                flash('Invalid username or password', 'danger')

        return render_template('login.html')

    @app.route('/index')
    @login_required
    def index():
        return render_template('index.html', username=session['username'])

    @app.route('/logout')
    def logout():
        session.pop('username', None)
        return redirect(url_for('login'))

    @app.route('/register', methods=['GET', 'POST'])
    def register():
        if request.method == 'POST':
            username_input = request.form['username']
            password_input = request.form['password']

            user_data = service.get_user(login=username_input)
            if user_data:
                flash('Username already exists.', 'danger')
            else:
                hashed_pw = generate_password_hash(password_input)
                service.create_user(login=username_input, password=hashed_pw,
                                    first_name=request.form['firstname'],
                                    last_name=request.form['lastname'])
                flash('Registration successful. Please log in.', 'success')
                return redirect(url_for('login'))
        return render_template('register.html')

    @app.route('/job_details')
    @login_required
    def job_details():
        name = request.args.get('name')
        return render_template('job_details.html', name=name)

    @app.route('/job_history')
    @login_required
    def job_history():
        name = request.args.get('name')
        return render_template('job_history.html', name=name)

    @app.route('/scheduler')
    @login_required
    def scheduler():
        return render_template('scheduler.html')

    @app.route('/create_application')
    @login_required
    def create_application():
        return render_template('create_app.html')

    @app.route('/create_job')
    @login_required
    def create_job():
        return render_template('create_job.html')

    @app.route('/status')
    @login_required
    def status():
        return service.status()

    @app.route('/jobs/all')
    @login_required
    def list_all_jobs():
        return service.list_all_jobs()

    @app.route('/jobs/run/', methods=['POST'])
    @login_required
    def run_job():
        username = session.get('username')
        content_type = request.headers.get('Content-Type')
        if content_type == 'application/json':
            request_data = json.loads(request.data.decode())
            return service.run_job(job_name=request_data['jobName'],
                                   submitter=username,
                                   job_parameters=json.loads(request_data['jobParameters']))
        else:
            return "Content type is not supported."

    @app.route('/jobs/<job_name>/app/details', methods=['GET'])
    @login_required
    def app_details(job_name: str):
        return service.application_details(job_name=job_name)

    @app.route('/jobs/<job_name>/details', methods=['GET'])
    @login_required
    def get_job_details(job_name: str):
        return service.job_details(job_name=job_name)

    @app.route('/jobs/history/<job_name>', methods=['GET'])
    @login_required
    def get_jobs_history(job_name: str):
        return service.jobs_history(job_name=job_name, is_current=False)

    @app.route('/jobs/status/<job_name>', methods=['GET'])
    @login_required
    def get_job_status(job_name: str):
        return service.jobs_history(job_name=job_name, is_current=True)

    @app.route('/executor/info', methods=['GET'])
    @login_required
    def executor_list():
        return service.executor_list()

    @app.route('/executor/info/<sch_name>/', methods=['GET'])
    @login_required
    def executor_info(sch_name: str):
        return service.executor_info(sch_name=sch_name)

    @app.route('/executor/<sch_name>/', methods=['PUT'])
    @login_required
    def executor_action(sch_name: str):
        content_type = request.headers.get('Content-Type')
        if content_type == 'application/json':
            request_data = json.loads(request.data.decode())
            return service.executor_action(sch_name=sch_name, action=request_data['action'])
        else:
            return "Content type is not supported."

    @app.route('/config/<element>/', methods=['GET'])
    @login_required
    def get_element_config(element: str):
        return service.get_element_config(element=element)

    @app.route('/app/names/', methods=['GET'])
    @login_required
    def get_app_names():
        return service.get_app_names()

    @app.route('/app/<app_id>/details', methods=['GET'])
    @login_required
    def get_app_details(app_id: str):
        return service.get_application_details(app_id=app_id)

    @app.route('/app/create/', methods=['POST'])
    @login_required
    def create_application_api():
        content_type = request.headers.get('Content-Type')
        if content_type == 'application/json':
            request_data = json.loads(request.data.decode())
            username = session.get('username')
            return service.create_application(app_data=request_data, user=username)
        else:
            return "Content type is not supported."

    @app.route('/job/create/', methods=['POST'])
    @login_required
    def create_job_api():
        content_type = request.headers.get('Content-Type')
        if content_type == 'application/json':
            request_data = json.loads(request.data.decode())
            username = session.get('username')
            return service.create_job(job_data=request_data, user=username)
        else:
            return "Content type is not supported."

    return app
