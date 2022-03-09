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
@click.option("--source", help="Connection string to source database", required=True)
@click.option("--destination", help="Connection string to destination database", required=True)
def click_command(source: str, destination: str):
    src_engine = create_engine(source)
    dst_engine = create_engine(destination)

    Source_Session = sessionmaker(bind=src_engine)
    Dest_Session = sessionmaker(bind=dst_engine)

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
