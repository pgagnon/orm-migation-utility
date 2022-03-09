import airflow.models as airflow_models

import click

from airflow.models import (
    Connection,
    DagRun,
    Pool,
    RenderedTaskInstanceFields,
    SensorInstance,
    SlaMiss,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    Trigger,
    Variable,
)

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine


@click.command()
@click.option("--source", help="Connection string to source database")
@click.option("--destination", help="Connection string to destination database")
def click_command(source: str, destination: str):
    Source_Session = sessionmaker(bind=create_engine(source))
    Dest_Session = sessionmaker(bind=create_engine(destination))

    for model in [
        Connection,
        DagRun,
        Pool,
        RenderedTaskInstanceFields,
        SensorInstance,
        SlaMiss,
        TaskFail,
        TaskInstance,
        TaskReschedule,
        Trigger,
        Variable,
    ]:
        src_session = Source_Session()
        dst_session = Dest_Session()
        objs = src_session.query(model).all()

        for obj in objs:
            dst_session.merge(obj)
            dst_session.commit()


if __name__ == "__main__":
    click_command()
