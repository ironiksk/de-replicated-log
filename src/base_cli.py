import sys
import os

from abc import abstractmethod

from dataclasses import dataclass
from argparse import ArgumentParser
import argparse

from typing import Optional, List, Dict
from omegaconf import OmegaConf, MISSING


class BaseCLI:

    @dataclass
    class Config:
        answer: int = 42

    config_prefix = None
    description = None

    @abstractmethod
    def main(self, config: Config):
        pass

    @classmethod
    def cli(cls):
        # Setup logging
        parser = argparse.ArgumentParser(description=cls.description)
        parser.add_argument('-c', '--config', type=str, default=None)
        parser.add_argument('-cp', '--config-prefix', type=str, default=None)
        parser.add_argument('-d', '--dump-config', action='store_true', help='Use this to seed initial config.yaml')
        parser.add_argument('-pp', '--params', type=str, nargs='*', default=[])
        args = parser.parse_args()
        print("args: ", args, file=sys.stderr)

        schema: cls.Config = OmegaConf.structured(cls.Config)
        config_cli: cls.Config = OmegaConf.from_cli(args.params)

        config_yaml: cls.Config = OmegaConf.from_dotlist([])
        if args.config:
            config_yaml = OmegaConf.load(args.config)
            if args.config_prefix or cls.config_prefix:
                config_yaml = config_yaml[args.config_prefix or cls.config_prefix]

        # NOTE: validation happens here. Doesn't validate for MISSING values
        config: cls.Config = OmegaConf.merge(schema, config_yaml, config_cli)

        print(f"==== default config: ====\n{OmegaConf.to_yaml(schema)}----", file=sys.stderr)
        if config_yaml:
            print(f"==== file config: ====\n{OmegaConf.to_yaml(config_yaml)}----", file=sys.stderr)
        print(f"==== cli config: ====\n{OmegaConf.to_yaml(config_cli)}----", file=sys.stderr)

        print(f"==== resulting config: ====\n{OmegaConf.to_yaml(config)}----", file=sys.stderr)

        if args.dump_config:
            print(f"Printing resulting config to stdout", file=sys.stderr)
            print(OmegaConf.to_yaml(config))
            return

        CLI = cls()
        return CLI.main(config)
