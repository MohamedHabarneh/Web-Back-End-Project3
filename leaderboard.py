import dataclasses
import sqlite3
import textwrap
import databases
import toml
from quart import Quart, abort, g, request
from quart_schema import QuartSchema, validate_request


app = Quart(__name__)
QuartSchema(app)

app.config.from_file(f"./etc/{__name__}.toml", toml.load)

async def _connect_db():
    database = databases.Database(app.config["DATABASES"]["URL"])
    await database.connect()
    return database


def _get_db():
    if not hasattr(g, "sqlite_db"):
        g.sqlite_db = _connect_db()
    return g.sqlite_db


@app.teardown_appcontext
async def close_connection(exception):
    db = getattr(g, "_sqlite_db", None)
    if db is not None:
        await db.disconnect()

# use redis and pass in dummy data
@app.route("/postgame", methods=["POST"])
async def postgame():
    auth = request.authorization
    if auth and auth.username and auth.password:
        #the code below is if you made a new database called leaderboard and use sqlite3
        # db = await _get_db()
        # game_result = dataclasses.asdict(data)
        # print("hello")
        # values = {"gameid": game_result["gameid"], "guesses": game_result["guesses"], "score" : (7-game_result["guesses"])}
        # print(values)
        # await db.execute("INSERT INTO leaderboard(gameid,guesses,score) VALUES(:gameid, :guesses, :score)",values,)
        return {"PostGame":"WORKS"}, 201
    else:
        return (
            {"error": "User not verified"},
            401,
            {"WWW-Authenticate": 'Basic realm = "Login required"'},
        )


@app.errorhandler(409)
def conflict(e):
    return {"error": str(e)}, 409