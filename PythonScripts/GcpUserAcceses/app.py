"""Flask app for inspecting and editing GCP IAM access.

Run:  python app.py   (then open http://127.0.0.1:5000)

Reads come from a local JSON cache (Refresh re-scans Cloud Asset Inventory).
Grant/revoke run gcloud as the logged-in user, verify the result, and patch
just the affected entry in the cache.
"""

from flask import Flask, jsonify, render_template, request

import gcp_iam

app = Flask(__name__)


@app.errorhandler(gcp_iam.AuthError)
def handle_auth_error(e):
    return jsonify({"error": str(e), "auth_required": True}), 401


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    data = gcp_iam.load_bindings_cache()
    return jsonify(data)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    data = gcp_iam.refresh_bindings_cache()
    # Best-effort refresh of the picker caches too (don't fail the scan on gcloud errors).
    try:
        gcp_iam.refresh_projects_cache()
    except Exception:
        pass
    try:
        gcp_iam.refresh_roles_cache()
    except Exception:
        pass
    return jsonify(data)


@app.route("/api/revoke", methods=["POST"])
def api_revoke():
    body = request.get_json(force=True)
    res = gcp_iam.revoke(
        member=body["member"],
        resource=body["resource"],
        role=body["role"],
        asset_type=body.get("asset_type", ""),
    )
    return jsonify(res)


@app.route("/api/grant", methods=["POST"])
def api_grant():
    body = request.get_json(force=True)
    res = gcp_iam.grant(
        member=body["member"],
        resource=body["resource"],
        role=body["role"],
        asset_type=body.get("asset_type", ""),
    )
    return jsonify(res)


@app.route("/api/pickers/projects")
def api_projects():
    refresh = request.args.get("refresh") == "1"
    data = gcp_iam.refresh_projects_cache() if refresh else gcp_iam.load_projects()
    return jsonify(data)


@app.route("/api/pickers/roles")
def api_roles():
    refresh = request.args.get("refresh") == "1"
    data = gcp_iam.refresh_roles_cache() if refresh else gcp_iam.load_roles()
    return jsonify(data)


@app.route("/api/pickers/resource-types")
def api_resource_types():
    project = request.args["project"]
    try:
        return jsonify({"project": project, "types": gcp_iam.list_resource_types(project)})
    except gcp_iam.AuthError:
        raise
    except Exception as e:  # surface gcloud/asset errors to the UI
        return jsonify({"error": str(e)}), 400


@app.route("/api/pickers/resources")
def api_resources():
    project = request.args["project"]
    asset_type = request.args["type"]
    try:
        return jsonify({"resources": gcp_iam.list_resources(project, asset_type)})
    except gcp_iam.AuthError:
        raise
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(debug=True, port=5000)
