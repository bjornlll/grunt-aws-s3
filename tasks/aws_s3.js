/*
 * grunt-aws-s3
 * https://github.com/MathieuLoutre/grunt-aws-s3
 *
 * Copyright (c) 2013 Mathieu Triay
 * Licensed under the MIT license.
 */

'use strict';
var zlib = require('zlib');
var tmpfile = require('temporary/lib/file');

// Local
var path = require('path');
var fs = require('fs');
var AWS = require('aws-sdk');
var mime = require('mime');

module.exports = function (grunt) {

	grunt.registerMultiTask('aws_s3', 'Upload files to AWS S3', function() {

		function verifyParams(params) {
			if (params) {
				if (!grunt.util._.every(grunt.util._.keys(params),
					function(key) { return grunt.util._.contains(put_params, key); })) {
					grunt.warn("params can only be " + put_params.join(', '));
				}
			}
		}

		var overrideOptions = (function() {

			function mime(task, local) {
				if (grunt.util._.isString(local)) {
					return local;
				} else if (grunt.util._.isObject(local)) {
					return grunt.util._.extend(
						{},
						(grunt.util._.isString(task) ? {} : task),
						local
					);
				} else {
					return task;
				}
			}

			function params(task, local) {
				var params = grunt.util._.extend({}, task, local);
				verifyParams(params);
				return params;
			}

			return function overrideOptions(taskOptions, localOptions) {
				var obj = localOptions ? {
					bucket: localOptions.bucket || taskOptions.bucket,
					access: localOptions.access || taskOptions.access,
					gzip: localOptions.gzip !== undefined ? localOptions.gzip : taskOptions.gzip,
					gzipExclude: localOptions.gzipExclude || taskOptions.gzipExclude,
					mime: mime(taskOptions.mime, localOptions.mime),
					params: params(taskOptions.params, localOptions.params)
				} : taskOptions;

				return obj;
			}

		})();

		var done = this.async();

		var options = this.options({
			access: 'public-read',
			accessKeyId: process.env.AWS_ACCESS_KEY_ID,
			secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
			concurrency: 1,
			mime: {},
			gzip: false,
			gzipExclude: []
		});

		var put_params = ['CacheControl', 'ContentDisposition', 'ContentEncoding', 
		'ContentLanguage', 'ContentLength', 'ContentMD5', 'Expires', 'GrantFullControl', 
		'GrantRead', 'GrantReadACP', 'GrantWriteACP', 'Metadata', 'ServerSideEncryption', 
		'StorageClass', 'WebsiteRedirectLocation'];
		
		if (!options.accessKeyId) {
			grunt.warn("Missing accessKeyId in options");
		}

		if (!options.secretAccessKey) {
			grunt.warn("Missing secretAccessKey in options");
		}

		if (!options.bucket) {
			grunt.warn("Missing bucket in options");
		}

		var s3_options = {
			bucket : options.bucket,
			accessKeyId : options.accessKeyId,
			secretAccessKey : options.secretAccessKey
		};

		if (!options.region) {
			grunt.log.writeln("No region defined, uploading to US Standard");
		} else {
			s3_options.region = options.region;
		}

		verifyParams(options.params);

		var s3 = new AWS.S3(s3_options);

		var dest;
		var isExpanded;
		var objects = [];

		this.files.forEach(function (filePair) {

			if (filePair.action === 'delete') {

				if (!filePair.dest) {
					grunt.fatal('No "dest" specified for deletion. No need to specify a "src"');
				}

				dest = (filePair.dest === '/') ? '' : filePair.dest;
				objects.push({
					dest: dest,
					action: 'delete',
					options: overrideOptions(options, filePair.orig.options)
				})
			} else {

				isExpanded = filePair.orig.expand || false;

				filePair.src.forEach(function (src) {
					
					if (grunt.util._.endsWith(dest, '/')) {	
						dest = (isExpanded) ? filePair.dest : unixifyPath(path.join(filePair.dest, src));
					} 
					else {
						dest = filePair.dest;
					}

					// '.' means that no dest path has been given (root).
					// We do not need to create a '.' folder
					if (dest !== '.') {
						objects.push({
							src: src,
							dest: dest, action: 'upload',
							options: overrideOptions(options, filePair.orig.options)
						});
					}
				});
			}
		});

		var deleteObjects = function (task, callback) {

			var clear = {
				Prefix: task.dest, 
				Bucket: options.bucket
			};

			s3.listObjects(clear, function (err, data) {

				if (!err) {

					if (data.Contents.length > 0) {

						var to_delete = {
							Objects: grunt.util._.map(data.Contents, function (o) { return {Key: o.Key} })
						};

						s3.deleteObjects({Delete: to_delete, Bucket: options.bucket}, function (err, data) {
							callback(err, data);
						});
					}
					else {
						callback(null, null)
					}
				}
				else {
					callback(err);
				}
			});
		}

		var uploadObject = function (task, callback) {

			var options = task.options;

			function getContentType(options, task) {
				return grunt.util._.isString(options.mime)
					? options.mime
					: (options.mime[task.srcOrig || task.src] || mime.lookup(task.srcOrig || task.src));
			}

			function gzip(task, callback) {
				var tmp = new tmpfile(),
					input = fs.createReadStream(task.src),
					output = fs.createWriteStream(tmp.path);
				task.srcOrig = task.src;
				input.pipe(zlib.createGzip()).pipe(output).on('error', function (err) {
					grunt.fail.warn("Upload failed", err);
				}).on('close', function () {
					task.src = tmp.path;
					callback(task, tmp);
				});
			}

			function shouldBeIncluded(options, task) {
				return options.gzipExclude.every(function(ext) { return task.src.indexOf(ext) === -1; });
			}

			function put(options, upload, callback) {
				var awsPutObjectParameters = grunt.util._.extend({}, options.params || {}, upload);
				s3.putObject(awsPutObjectParameters, function (err, data) {
					callback(err, data);
				});
			}

			if (grunt.file.isDir(task.src)) {
				if (!grunt.util._.endsWith(task.dest, '/')) {
					task.dest = task.dest + '/';
				}
				put({}, {
					Key: task.dest,
					Bucket: options.bucket,
					ACL: options.access
				}, callback);
			} else if (options.gzip && shouldBeIncluded(options, task)) {
				gzip(task, function(task, tmp) {
					put(task.options, {
						ContentType: getContentType(options, task),
						ContentEncoding: 'gzip',
						Body: grunt.file.read(task.src, {encoding: null}),
						Key: task.dest,
						Bucket: options.bucket,
						ACL: options.access
					}, function(err, data) {
						tmp.unlinkSync();
						callback(err, data);
					})
				});
			} else {
				put(task.options, {
					ContentType: getContentType(options, task),
					Body: grunt.file.read(task.src, {encoding: null}),
					Key: task.dest,
					Bucket: options.bucket,
					ACL: options.access
				}, callback);
			}

		}

		var queue = grunt.util.async.queue(function (task, callback) {
			
			if (task.action === 'delete') {
				deleteObjects(task, callback);
			}
			else {
				uploadObject(task, callback);
			}
		}, options.concurrency);

		queue.drain = function () {
			var tally = grunt.util._.groupBy(objects, 'action');

			if (tally.upload) {
				grunt.log.writeln('\n' + tally.upload.length.toString().green + ' objects uploaded on the bucket ' + options.bucket.toString().green);				
			}

			grunt.util._.each(tally['delete'], function (del) {
				grunt.log.writeln(del.nb_objects.toString().green + ' objects deleted at ' + (options.bucket + '/' + del.dest).toString().green);
			});

			done();
		};

		queue.push(objects, function (err, res) {

			var objectURL = s3.endpoint.href + options.bucket + '/' + this.data.dest;

			if (this.data.action === 'delete') {
				
				if (err) {
					
					if (res && res.Errors.length > 0) {
						grunt.writeln('Errors (' + res.Errors.length.toString().red + ' objects): ' + grunt.util._.pluck(res.Errors, 'Key').join(', ').toString().red);
					}

					grunt.fatal('Failed to delete content of ' + objectURL + '\n' + err);
				}
				else {

					if (res) {
						grunt.log.writeln('Successfuly deleted the content of ' + objectURL.toString().cyan);
						grunt.log.writeln('List: (' + res.Deleted.length.toString().cyan + ' objects): '+ grunt.util._.pluck(res.Deleted, 'Key').join(', ').toString().cyan);
						this.data.nb_objects = res.Deleted.length;
					}
					else {
						grunt.log.writeln('Nothing to delete in ' + objectURL.toString().cyan);
						this.data.nb_objects = 0;
					}
				}
			}
			else {
				if (err) {
					grunt.fatal('Failed to upload ' + this.data.src.toString().cyan + ' to ' + objectURL.toString().cyan + ".\n" + err.toString().red);
				}
				else {
					grunt.log.writeln(this.data.src.toString().cyan + ' uploaded to ' + objectURL.toString().cyan);
				}
			}
		});
	});

	var unixifyPath = function (filepath) {
		
		if (process.platform === 'win32') {
			return filepath.replace(/\\/g, '/');
		} 
		else {	
			return filepath;
		}
	};
};
