from sqlalchemy import text

from humaniki_schema import db
from humaniki_schema.log import get_logger
from humaniki_schema.queries import update_fill_detail
from datetime import datetime as dt

log = get_logger()

# the tables that store full copies of the base data each week. the metrics never refer back to them.
# I used to think i wanted to keep these full copies to look at interesting moments when something happened, but space is more important
# I also made the mistake of not partitioning these tables on the fill_id, so
DELETABLE_TABLES = ['human_sitelink', 'human', 'human_country', 'human_occupation']


def delete_single_fill(db_session, fill_id):
    # choosing to do this one fill at a time because otherwise performance can be slow, and i want to log in between
    deletion_status = {}
    for deleteable_table in DELETABLE_TABLES:
        try:
            deletion_q =                 f"""DELETE FROM {deleteable_table}
                    where fill_id = {fill_id}"""
            log.info(f'now executing deletion SQL: {deletion_q}')
            db_session.execute(text(deletion_q))
            deletion_status[deleteable_table] = 'deleted'
        except Exception as e:
            deletion_status[deleteable_table] = str(e)
    # update
    update_fill_detail(db_session, fill_id, 'deletion_status', deletion_status)
    update_fill_detail(db_session, fill_id, 'deleted_date', dt.now().strftime('%Y-%m-%d'))


def delete_database_data(retention_days, max_fills_to_delete=5):
    """delete some rows of old fills, and turn them inactive"""
    log.info(f"deleting database data older than {retention_days}")
    db_session = db.session_factory()

    # get the fills needing deleting
    fill_ids_needing_deletion_rows = db_session.execute(text(
        f"""SELECT id
            FROM fill
            WHERE date < DATE_SUB( CURDATE(), INTERVAL {retention_days} DAY) -- interpolate retention days
            AND detail->'$.deleted_date' is null -- hasn't been deleted before"""
    )).all()
    fill_ids_needing_deletion = [row[0] for row in fill_ids_needing_deletion_rows]
    log.info(f'{len(fill_ids_needing_deletion)} fills needing deletion: {fill_ids_needing_deletion}')

    fills_deleted = 0
    for fill_id in fill_ids_needing_deletion:
        if fills_deleted < max_fills_to_delete:
            log.info(f'now deleting: {fill_id}')
            delete_single_fill(db_session, fill_id)
            fills_deleted += 1
        else:
            log.info(f'Deleting just {max_fills_to_delete} for today')
            return

    db_session.commit()


if __name__=='__main__':
    delete_database_data(90)
