import click
from block_cascade.cli.crud import (
    create_persistent_resource,
    list_persistent_resources,
    delete_persistent_resource,
    list_active_jobs
)


@click.group()
@click.version_option()
def cli():
    """Create and manage persistent resources."""
    pass


# Add commands to the CLI group
cli.add_command(create_persistent_resource)
cli.add_command(list_persistent_resources)
cli.add_command(delete_persistent_resource)
cli.add_command(list_active_jobs)

if __name__ == "__main__":
    cli()
