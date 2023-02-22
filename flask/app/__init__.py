from flask import Flask
from flask_restful import Api
from .config import SECRET_KEY
from app.resources import UserResource, LeadResource, UserLoginResource, LeadResultResource, LeadPulloutResouce


def create_app():
    app = Flask(__name__)
    app.secret_key = SECRET_KEY
    api = Api(app, prefix='/api')

    api.add_resource(UserResource, '/register')
    api.add_resource(UserLoginResource, '/login')
    api.add_resource(LeadResource, '/get_accounts', '/leads')
    api.add_resource(LeadPulloutResouce, '/leads')
    api.add_resource(LeadResultResource, '/status')

    return app
