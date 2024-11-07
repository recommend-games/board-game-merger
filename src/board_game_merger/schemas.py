import polars as pl

GAME_ITEM_SCHEMA = pl.Schema(
    {  # type: ignore[arg-type]
        "name": pl.String,
        "alt_name": pl.List(pl.String),
        "year": pl.Int64,
        "game_type": pl.List(pl.String),
        "description": pl.String,
        "designer": pl.List(pl.String),
        "artist": pl.List(pl.String),
        "publisher": pl.List(pl.String),
        "url": pl.String,
        "official_url": pl.List(pl.String),
        "image_url": pl.List(pl.String),
        "image_url_download": pl.List(pl.String),
        "image_file": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                },
            ),
        ),
        "image_blurhash": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                    "blurhash": pl.String,
                },
            ),
        ),
        "video_url": pl.List(pl.String),
        "rules_url": pl.List(pl.String),
        "rules_file": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                },
            ),
        ),
        "review_url": pl.List(pl.String),
        "external_link": pl.List(pl.String),
        "list_price": pl.String,
        "min_players": pl.Int64,
        "max_players": pl.Int64,
        "min_players_rec": pl.Int64,
        "max_players_rec": pl.Int64,
        "min_players_best": pl.Int64,
        "max_players_best": pl.Int64,
        "min_age": pl.Int64,
        "max_age": pl.Int64,
        "min_age_rec": pl.Float64,
        "max_age_rec": pl.Float64,
        "min_time": pl.Int64,
        "max_time": pl.Int64,
        "category": pl.List(pl.String),
        "mechanic": pl.List(pl.String),
        "cooperative": pl.Boolean,
        "compilation": pl.Boolean,
        # "compilation_of": pl.List(pl.Int64),
        "family": pl.List(pl.String),
        "expansion": pl.List(pl.String),
        "implementation": pl.List(pl.Int64),
        "integration": pl.List(pl.Int64),
        "rank": pl.Int64,
        "add_rank": pl.List(
            pl.Struct(
                {
                    "game_type": pl.String,
                    "game_type_id": pl.Int64,
                    "name": pl.String,
                    "rank": pl.Int64,
                    "bayes_rating": pl.Float64,
                },
            ),
        ),
        "num_votes": pl.Int64,
        "avg_rating": pl.Float64,
        "stddev_rating": pl.Float64,
        "bayes_rating": pl.Float64,
        "worst_rating": pl.Int64,
        "best_rating": pl.Int64,
        "complexity": pl.Float64,
        "easiest_complexity": pl.Int64,
        "hardest_complexity": pl.Int64,
        "language_dependency": pl.Float64,
        "lowest_language_dependency": pl.Int64,
        "highest_language_dependency": pl.Int64,
        "bgg_id": pl.Int64,
        "freebase_id": pl.String,
        "wikidata_id": pl.String,
        "wikipedia_id": pl.String,
        "dbpedia_id": pl.String,
        "luding_id": pl.Int64,
        "spielen_id": pl.String,
        "published_at": pl.String,
        "updated_at": pl.String,
        "scraped_at": pl.String,
    },
)


USER_ITEM_SCHEMA = pl.Schema(
    {  # type: ignore[arg-type]
        "item_id": pl.Int64,
        "bgg_user_name": pl.String,
        "first_name": pl.String,
        "last_name": pl.String,
        "registered": pl.Int64,
        "last_login": pl.String,
        "country": pl.String,
        "region": pl.String,
        "city": pl.String,
        "external_link": pl.List(pl.String),
        "image_url": pl.List(pl.String),
        "image_url_download": pl.List(pl.String),
        "image_file": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                },
            ),
        ),
        "image_blurhash": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                    "blurhash": pl.String,
                },
            ),
        ),
        "published_at": pl.String,
        "updated_at": pl.String,
        "scraped_at": pl.String,
    },
)

RATING_ITEM_SCHEMA = pl.Schema(
    {
        "item_id": pl.String,
        "bgg_id": pl.Int64,
        "bgg_user_name": pl.String,
        "bgg_user_rating": pl.Float64,
        "bgg_user_owned": pl.Boolean,
        "bgg_user_prev_owned": pl.Boolean,
        "bgg_user_for_trade": pl.Boolean,
        "bgg_user_want_in_trade": pl.Boolean,
        "bgg_user_want_to_play": pl.Boolean,
        "bgg_user_want_to_buy": pl.Boolean,
        "bgg_user_preordered": pl.Boolean,
        "bgg_user_wishlist": pl.Int64,
        "bgg_user_play_count": pl.Int64,
        "comment": pl.String,
        "published_at": pl.String,
        "updated_at": pl.String,
        "scraped_at": pl.String,
    },
)

ITEM_TYPE_SCHEMA = {
    "GameItem": GAME_ITEM_SCHEMA,
    "UserItem": USER_ITEM_SCHEMA,
    "RatingItem": RATING_ITEM_SCHEMA,
}
