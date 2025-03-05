from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/', methods=['GET','POST'])
def home():
    return jsonify({"message":"api: success"})

@app.route('/hello', methods=['GET', 'POST'])
def hello():
    if request.method == 'GET':
        name = request.args.get('name')
    elif request.method == 'POST':
        data = request.get_json()
        name = data.get('name') if data else None

    if not name:
        return jsonify({"error": "Name is required (e.g. 'localhost:5001/hello?name=Shiv')"}), 400

    greeting = f"Hello {name}!"

    return jsonify({"message": greeting})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)