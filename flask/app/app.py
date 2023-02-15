from flask import Flask, request, jsonify, make_response
from flask_restful import Api

from resources import UserResource,LeadResource

def create_api():
    app = Flask(__name__)
    api = Api(app, prefix='/api')

    api.add_resource(UserResource, '/closers/<int:id>', '/closers')
    api.add_resource(LeadResource, '/get_accounts/')

    # Add any other configuration or middleware here

    return app
