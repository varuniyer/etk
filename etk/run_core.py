import time
import json
import codecs
import sys
import multiprocessing as mp, os
import core
from argparse import ArgumentParser
import requests
# # from concurrent import futures
# from pathos.multiprocessing import ProcessingPool
# from pathos import multiprocessing as mpp
# import multiprocessing as mp
# import pathos
# # from pathos.helpers
import gzip
""" Process code begins here """


# def output_write(output_path):
#     return codecs.open(output_path, 'w+')
#
#
# def chunk_file(file_name, size=1024 * 1024):
#     """ Splitting data into chunks for parallel processing
#     :param file_name - name of the file to split
#     :param size - size of file to split
#     """
#     file_end = os.path.getsize(file_name)
#     with open(file_name, 'r') as f:
#         chunk_end = f.tell()
#         while True:
#             chunk_start = chunk_end
#             f.seek(size, 1)
#             f.readline()
#             chunk_end = f.tell()
#             yield chunk_start, chunk_end - chunk_start
#             if chunk_end > file_end:
#                 break
#
#
# def process_wrapper(core, input, chunk_start, chunk_size, queue):
#     results = []
#     with open(input) as f:
#         f.seek(chunk_start)
#         lines = f.read(chunk_size).splitlines()
#         for i, line in enumerate(lines):
#             document = json.loads(line)
#             try:
#                 document = core.process(document, create_knowledge_graph=True)
#             except Exception as e:
#                 print "Failed - ", e
#
#             # queue.put(json.dumps(document))
#             # print "Processing chunk - ", str(chunk_start), " File - ", str(i)
#
#
# def listener(queue, output):
#     f = open(output, 'wb')
#     while 1:
#         message = queue.get()
#         if message == 'kill':
#             print "Done writing to file......."
#             break
#         f.write(message + '\n')
#         f.flush()
#     f.close()
#
#
# def run_parallel(input, output, core, processes=0):
#     processes = processes or mp.cpu_count()
#     processes += 2 # for writing
#
#     manager = mp.Manager()
#     queue = manager.Queue()
#     pool = mp.Pool(processes)
#
#     # put listener to work first
#     watcher = pool.apply_async(listener, (queue, output))
#
#     jobs = []
#
#     for chunk_start, chunk_size in chunk_file(input):
#         jobs.append(pool.apply_async(process_wrapper, (core, input, chunk_start, chunk_size, queue)))
#     for job in jobs:
#         job.get()
#     queue.put('kill')
#     pool.close()


def run_serial(input, output, core, prefix=''):
    output = codecs.open(output, 'w')
    index = 0
    for line in codecs.open(input):
        print prefix, 'processing line number:', index
        start_time_doc = time.time()
        jl = json.loads(line)
        result = core.process(jl, create_knowledge_graph=True)
        if result:
            output.write(json.dumps(result) + '\n')
            time_taken_doc = time.time() - start_time_doc
            if time_taken_doc > 5:
                print prefix, "Took", str(time_taken_doc), " seconds"
        else:
            print 'Failed line number:', index
        index += 1
    output.close()


def run_serial_in_memory(input, core, size, http_url, http_headers, dump_path, worker_id, prefix):

    index = 0
    chunk_count = 0
    data_chunk = []

    for line in codecs.open(input):
        print prefix, 'processing line number:', index
        jl = json.loads(line)
        result = core.process(jl, create_knowledge_graph=True)
        if result:
            # write result to memory
            data_chunk.append(json.dumps(result))
            # upload to http service
            if len(data_chunk) >= size:
                upload(data_chunk, http_url, http_headers, dump_path, worker_id, chunk_count)
                data_chunk = []
                chunk_count += 1
        else:
            print 'Failed line number:', index

        index += 1

    # upload rest data
    if len(data_chunk) > 0:
        upload(data_chunk, http_url, http_headers, dump_path, worker_id, chunk_count)


def upload(data_chunk, http_url, http_headers, dump_path, worker_id, chunk_count):
    data_to_upload = '\n'.join(data_chunk) + '\n'
    resp = requests.post(http_url, data=data_to_upload, headers=http_headers)
    # if it fails, dump to file
    if resp.status_code // 100 != 2:
        print resp.status_code, resp.content
        dump_file_path = os.path.join(dump_path, 'dumped_{}_{}.jl'.format(worker_id, chunk_count))
        with codecs.open(dump_file_path, 'w') as f:
            f.write(data_to_upload)

# def process_one(x):
#     # output = "output-%d.gz" % pathos.helpers.mp.current_process().getPid()
#     output = c_options.outputPath + "/output-%d.jl" % mp.current_process().pid
#     with codecs.open(output, "a+") as out:
#         out.write('%s\n' % json.dumps(c.process(x)))
#
# def run_parallel_2(input_path, output_path, core, processes=0):
#     lines = codecs.open(input_path, 'r').readlines()
#     inputs = list()
#     # pool = ProcessingPool(16)
#     pool = mpp.Pool(8)
#     for line in lines:
#         inputs.append(json.loads(line))
#     # pool = .ProcessPoolExecutor(max_workers=8)
#     # results = list(pool.map(process_one, inputs))
#     pool.map(process_one, inputs)
#
#     # output_f = codecs.open(output_path, 'w')
#     # for result in results:
#     #     output_f.write(json.dumps(result))
#     #     output_f.write('\n')


def run_parallel_3(input_path, output_path, config_path, processes, batch=None):
    if not os.path.exists(output_path) or not os.path.isdir(output_path) :
        raise Exception('output path is invalid')
    # if len(os.listdir(temp_path)) != 0:
    #     raise Exception('temp path is not empty')
    if processes < 1:
        raise Exception('invalid process number')

    if batch is not None:
        if not os.path.exists(batch['dump_path']) or not os.path.isdir(batch['dump_path']):
            raise Exception('batch output dir path is invalid')


    # split input file into chunks
    print 'splitting input file...'
    with codecs.open(input_path, 'r') as input:
        input_chunk_file_handlers = [
            codecs.open(os.path.join(output_path, 'input_chunk_{}.json'.format(i)), 'w') for i in xrange(processes)]
        idx = 0
        for line in input:
            if line == '\n':
                continue
            input_chunk_file_handlers[idx].write(line)
            idx = (idx + 1) % processes
        for f in input_chunk_file_handlers:
            f.close()

    # create processes
    print 'creating workers...'
    print '-------------------'
    process_handlers = []
    for i in xrange(processes):
        input_chunk_path = os.path.join(output_path, 'input_chunk_{}.json'.format(i))
        output_chunk_path = os.path.join(output_path, 'output_chunk_{}.json'.format(i))
        p = mp.Process(target=run_parallel_worker,
                   args=(i, input_chunk_path, output_chunk_path, config_path, batch))
        process_handlers.append(p)

    # start processes
    for p in process_handlers:
        p.start()

    # wait till finish
    for p in process_handlers:
        p.join()

    print '-------------------'


def run_parallel_worker(worker_id, input_chunk_path, output_chunk_path, config_path, batch):
    print 'start worker #{}'.format(worker_id)
    c = core.Core(json.load(codecs.open(config_path, 'r')))
    if batch is None:
        run_serial(input_chunk_path, output_chunk_path, c, prefix='worker {}:'.format(worker_id))
    else:
        run_serial_in_memory(input_chunk_path, c,
            batch['size'], batch['http_url'], batch['http_headers'], batch['dump_path'],
            worker_id, 'worker #{}'.format(worker_id))
    print 'worker #{} finished'.format(worker_id)

def usage():
    return """\
Usage: python run_core.py [args]
-i, --input <input_doc>                   Input file
-o, --output <output_doc>                 Output file
-c, --config <config>                     Config file

Optional
-m, --multiprocessing-enabled
-t, --multiprocessing-processes <processes>    Serial(default=0)
                                               Run Parallel(>0)--batch-enabled", action="store_true", dest="batchEnabled")
--batch-enabled
--batch-size <size>
--batch-http-url <url>
--batch-http-headers <json>
--batch-dump-path <path>
    """

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", action="store", type=str, dest="inputPath")
    parser.add_argument("-o", "--output", action="store", type=str, dest="outputPath")
    parser.add_argument("-c", "--config", action="store", type=str, dest="configPath")

    parser.add_argument("-m", "--multiprocessing-enabled", action="store_true", dest="mpEnabled")
    parser.add_argument("-t", "--multiprocessing-processes", action="store",
                      type=int, dest="mpProcesses", default=mp.cpu_count())

    parser.add_argument("--batch-enabled", action="store_true", dest="batchEnabled")
    parser.add_argument("--batch-size", action="store", type=int, dest="batchSize", default=100)
    parser.add_argument("--batch-http-url", action="store", type=str, dest="batchHttpUrl")
    parser.add_argument("--batch-http-headers", action="store", type=str, dest="batchHttpHeaders")
    parser.add_argument("--batch-dump-path", action="store", type=str, dest="batchDumpPath")

    c_options, args = parser.parse_known_args()

    # print c_options.batchHttpUrl, c_options.batchSize, c_options.batchHttpHeaders, c_options.batchDirPath
    # print type(c_options.batchHttpHeaders)
    # print json.loads(c_options.batchHttpHeaders)
    # print c_options.batchEnabled
    # sys.exit()

    if not (c_options.inputPath and c_options.outputPath and c_options.configPath):
        print usage()
        sys.exit()
    try:
        start_time = time.time()
        if c_options.mpEnabled and c_options.mpProcesses > 1:
            print "processing parallelly"
            if not c_options.batchEnabled:
                run_parallel_3(
                    input_path=c_options.inputPath,
                    output_path=c_options.outputPath,
                    config_path=c_options.configPath,
                    processes=c_options.mpProcesses)
            else:
                # write into memory
                if not (c_options.batchHttpUrl and c_options.batchSize and
                                c_options.batchHttpHeaders and c_options.batchDumpPath):
                    print usage()
                    sys.exit()
                batch = {
                    'size': c_options.batchSize,
                    'http_url': c_options.batchHttpUrl,
                    'http_headers': json.loads(c_options.batchHttpHeaders),
                    'dump_path': c_options.batchDumpPath
                }
                run_parallel_3(
                    input_path=c_options.inputPath,
                    output_path=c_options.outputPath,
                    config_path=c_options.configPath,
                    processes=c_options.mpProcesses,
                    batch=batch)
        else:
            print "processing serially"
            c = core.Core(json.load(codecs.open(c_options.configPath, 'r')))
            run_serial(c_options.inputPath, c_options.outputPath, c)
        print('The script took {0} second !'.format(time.time() - start_time))

    except Exception as e:
        print e


