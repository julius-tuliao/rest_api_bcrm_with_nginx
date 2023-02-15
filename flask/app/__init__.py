from flask import Flask
from flask_restful import Api

from app.resources import UserResource, LeadResource, UserLoginResource, LeadResultResource


def create_app():
    app = Flask(__name__)

    api = Api(app, prefix='/api')

    api.add_resource(UserResource, '/register')
    api.add_resource(UserLoginResource, '/login')
    api.add_resource(LeadResource, '/get_accounts/')
    api.add_resource(LeadResultResource, '/status/')

    return app
