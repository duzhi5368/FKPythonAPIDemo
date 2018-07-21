from flask import Flask, request, render_template, jsonify
import json

app = Flask(__name__)
print type(app)

@app.route('/')
def index():
    a = []
    a.append({ "1": 2})
    print(a)
    return jsonify(a)

@app.route('/hello')
def hello():
    return 'Hello World'

@app.route('/user/<username>')
def show_user_profile(username):
    # show the user profile for that user
    return 'UserName = %s' % username

@app.route('/post/<int:post_id>')
def show_post(post_id):
    # show the post with the given id, the id is an integer
    return 'PostID = %d' % post_id

@app.errorhandler(403)
def err_403(error):
    app.logger.error('Resource forbidden : %s , client ip : %s', request.path, request.remote_addr)
    return 'not allowed !'


@app.errorhandler(404)
def err_404(error):
    app.logger.error('Page not found: %s , client ip : %s', request.path, request.remote_addr)
    return 'yes'#render_template('error.html'), 404


@app.errorhandler(500)
def err_500(error):
    app.logger.error('500 error found: %s , client ip : %s', request.path, request.remote_addr)
    return 'server error happened !'

"""
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        searchword = request.args.get('q', '')
        app.logger.debug(request.args)
        return "post sth. " + searchword
    else:
        searchword = request.args.get('q', '')
        app.logger.info(request.args)
        return "get sth. " + searchword
"""
if __name__ == "__main__":
    a = []
    a.append({1:2})
    print(a)
    app.run(host='0.0.0.0', port=5000, debug=True)