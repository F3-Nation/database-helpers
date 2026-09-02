"""Flask app for visualizing and editing Postgres security.

Run:  python app.py   (then open http://127.0.0.1:5001)

Reads come from a per-(service, database) JSON cache; Refresh re-introspects.
Grant/revoke run real SQL, verify against the catalog, and patch the cache.
"""

from flask import Flask, jsonify, render_template, request

import pg_security as pg

app = Flask(__name__)


@app.errorhandler(pg.ConnError)
def handle_conn_error(e):
    return jsonify({"error": str(e), "conn_error": True}), 503


@app.errorhandler(pg.InputError)
def handle_input_error(e):
    return jsonify({"error": str(e)}), 400


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/services")
def api_services():
    return jsonify({
        "services": pg.list_services(),
        "default_service": pg.DEFAULT_SERVICE,
        "default_dbname": pg.DEFAULT_DBNAME,
    })


@app.route("/api/databases")
def api_databases():
    service = request.args["service"]
    return jsonify({"databases": pg.list_databases(service)})


@app.route("/api/data")
def api_data():
    service = request.args["service"]
    dbname = request.args["dbname"]
    return jsonify(pg.load_snapshot(service, dbname))


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    body = request.get_json(force=True)
    return jsonify(pg.refresh_snapshot(body["service"], body["dbname"]))


@app.route("/api/grant-membership", methods=["POST"])
def api_grant_membership():
    b = request.get_json(force=True)
    return jsonify(pg.grant_membership(
        b["service"], b["dbname"], b["group_role"], b["member_role"],
        b.get("admin_option", False),
    ))


@app.route("/api/revoke-membership", methods=["POST"])
def api_revoke_membership():
    b = request.get_json(force=True)
    return jsonify(pg.revoke_membership(
        b["service"], b["dbname"], b["group_role"], b["member_role"],
    ))


@app.route("/api/grant-privilege", methods=["POST"])
def api_grant_privilege():
    b = request.get_json(force=True)
    return jsonify(pg.grant_privilege(
        b["service"], b["dbname"], b["object_type"], b.get("schema"),
        b.get("object"), b.get("args"), b["grantee"], b["privilege"],
        b.get("grant_option", False),
    ))


@app.route("/api/revoke-privilege", methods=["POST"])
def api_revoke_privilege():
    b = request.get_json(force=True)
    return jsonify(pg.revoke_privilege(
        b["service"], b["dbname"], b["object_type"], b.get("schema"),
        b.get("object"), b.get("args"), b["grantee"], b["privilege"],
    ))


@app.route("/api/pickers/schemas")
def api_schemas():
    return jsonify({"schemas": pg.list_schemas(request.args["service"], request.args["dbname"])})


@app.route("/api/pickers/objects")
def api_objects():
    a = request.args
    return jsonify({"objects": pg.list_objects(
        a["service"], a["dbname"], a["schema"], a["type"],
    )})


@app.route("/api/pickers/privileges")
def api_privileges():
    return jsonify({"privileges_by_type": pg.PRIVILEGES_BY_TYPE})


if __name__ == "__main__":
    app.run(debug=True, port=5001)
