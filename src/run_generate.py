import argparse
from generate import generate_patient_db


def get_command_line_args():
    parser = argparse.ArgumentParser()

    # Bools
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--use_dask', action='store_true')
    parser.add_argument('--sample_column_values', action='store_true')

    # Paths
    parser.add_argument('--demographics_path',
                        default='/share/pi/stamang/covid/data/demo/'
                                'demo_all_pts.parquet')

    # Directories
    #parser.add_argument(
    #    '--meddra_extractions_dir',
    #    default='/share/pi/stamang/covid/data/notes_20190901_20200701/'
    #            'labeled_extractions')
    parser.add_argument(
        '--meddra_extractions_dir',
        default='/home/colbyham/colby_data/notes_20190901_20200701/'
                'labeled_extractions')
    parser.add_argument(
        '--drug_exposure_dir',
        default='/share/pi/stamang/covid/data/drug_exposure')
    parser.add_argument(
        '--concept_dir',
        default='/share/pi/stamang/covid/data/concept')
    parser.add_argument(
        '--output_dir',
        default='/home/colbyham/output/patient_db',
        help='Path to output directory')  # , required=True)

    args: argparse.Namespace = parser.parse_args()
    return args


def main(args):
    print(f"\nargs: {args}\n")
    generate_patient_db(args.demographics_path, args.meddra_extractions_dir,
                        args.drug_exposure_dir, args.concept_dir,
                        args.output_dir, args.debug, args.use_dask)


if __name__ == '__main__':
    main(get_command_line_args())
