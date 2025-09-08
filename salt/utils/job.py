"""
Functions for interacting with the job cache
"""


import logging

import salt.minion
import salt.utils.event
import salt.utils.jid
import salt.utils.verify
import salt.utils.versions

log = logging.getLogger(__name__)


def store_job(opts, load, event=None, mminion=None):
    """
    Store job information using the configured master_job_cache
    """
    # Generate EndTime
    endtime = salt.utils.jid.jid_to_time(salt.utils.jid.gen_jid(opts))
    # If the return data is invalid, just ignore it
    if any(key not in load for key in ("return", "jid", "id")):
        return False
    if not salt.utils.verify.valid_id(opts, load["id"]):
        return False
    if mminion is None:
        mminion = salt.minion.MasterMinion(opts, states=False, rend=False)

    # When a minion-generated job (salt-call or scheduler) comes through this
    # function to write the job to the cache, the expectation is that (since
    # the job was not initiated from a master), it will not have a JID yet, and
    # therefore the "jid" key in the payload will be set to "req"... a signal
    # that prep_jid must be invoked to ask the master to create a JID. However,
    # in cases where additional returners are specified by the minion for a
    # given job, these additional returners are invoked before we get to this
    # point.
    #
    # For any returner to save a job, a JID is required, so the use of
    # additional returners by necessity will allocate a JID for the job.
    # Therefore, in these cases where additional returners are specified, the
    # payload entering this function will already have a JID (i.e. it will not
    # be set to "req"). As a result, the "arg", "tgt_type", and "tgt" values
    # will not be added to the payload before it is written to the job cache.
    #
    # This is a problem; in the default master_job_cache backend (local_cache),
    # when the "save_load" func is invoked to write the payload to disk, it
    # checks the payload for a "tgt" value. If present, then the master will
    # find the minions matching that target, and write a list of minions to the
    # job cache in a file called ".minions.p". For reference, here is where
    # this happens:
    #
    # https://github.com/saltstack/salt/blob/v3006.6/salt/returners/local_cache.py#L218-L227
    #
    # The reason for conditionally writing this file is that the results of
    # runner functions executed via salt-run are also written to the job cache,
    # but as runners are executed master-side, they do not involve a minion
    # target, and thus ".minions.p" is irrelevant.
    #
    # However, the absence of a ".minions.p" breaks Salt's ability to look up
    # results for the JID, as these lookups require this file to be present,
    # which you can see here:
    #
    # https://github.com/saltstack/salt/blob/v3006.6/salt/returners/local_cache.py#L306-L312
    #
    # The absence of this file results in a traceback on any job cache lookups
    # for this JID, which in turn impedes the syndic's ability to forward
    # results for the job to the syndic_master.
    #
    # Long story long, this is a bug in Salt. There is likely a more elegant
    # fix, but for our purposes, the following block of code will ensure that
    # these keys are present in the payload.
    #
    if load["jid"] != "req":
        if "arg" not in load:
            load["arg"] = load.get("fun_args", [])
        if "tgt_type" not in load:
            load["tgt_type"] = "glob"
        if "tgt" not in load:
            load["tgt"] = load["id"]

    job_cache = opts["master_job_cache"]
    if load["jid"] == "req":
        # The minion is returning a standalone job, request a jobid
        load["arg"] = load.get("arg", load.get("fun_args", []))
        load["tgt_type"] = "glob"
        load["tgt"] = load["id"]

        prep_fstr = "{}.prep_jid".format(opts["master_job_cache"])
        try:
            load["jid"] = mminion.returners[prep_fstr](
                nocache=load.get("nocache", False)
            )
        except KeyError:
            emsg = "Returner '{}' does not support function prep_jid".format(job_cache)
            log.error(emsg)
            raise KeyError(emsg)
        except Exception:  # pylint: disable=broad-except
            log.critical(
                "The specified '%s' returner threw a stack trace:\n",
                job_cache,
                exc_info=True,
            )

        # save the load, since we don't have it
        saveload_fstr = "{}.save_load".format(job_cache)
        try:
            mminion.returners[saveload_fstr](load["jid"], load)
        except KeyError:
            emsg = "Returner '{}' does not support function save_load".format(job_cache)
            log.error(emsg)
            raise KeyError(emsg)
        except Exception:  # pylint: disable=broad-except
            log.critical(
                "The specified '%s' returner threw a stack trace",
                job_cache,
                exc_info=True,
            )
    elif salt.utils.jid.is_jid(load["jid"]):
        # Store the jid
        jidstore_fstr = "{}.prep_jid".format(job_cache)
        try:
            mminion.returners[jidstore_fstr](False, passed_jid=load["jid"])
        except KeyError:
            emsg = "Returner '{}' does not support function prep_jid".format(job_cache)
            log.error(emsg)
            raise KeyError(emsg)
        except Exception:  # pylint: disable=broad-except
            log.critical(
                "The specified '%s' returner threw a stack trace",
                job_cache,
                exc_info=True,
            )

    if event:
        # If the return data is invalid, just ignore it
        log.info("Got return from %s for job %s", load["id"], load["jid"])
        event.fire_event(
            load, salt.utils.event.tagify([load["jid"], "ret", load["id"]], "job")
        )
        event.fire_ret_load(load)

    # if you have a job_cache, or an ext_job_cache, don't write to
    # the regular master cache
    if not opts["job_cache"] or opts.get("ext_job_cache"):
        return

    # do not cache job results if explicitly requested
    if load.get("jid") == "nocache":
        log.debug(
            "Ignoring job return with jid for caching %s from %s",
            load["jid"],
            load["id"],
        )
        return

    # otherwise, write to the master cache
    savefstr = "{}.save_load".format(job_cache)
    getfstr = "{}.get_load".format(job_cache)
    fstr = "{}.returner".format(job_cache)
    updateetfstr = "{}.update_endtime".format(job_cache)
    if "fun" not in load and load.get("return", {}):
        ret_ = load.get("return", {})
        if "fun" in ret_:
            load.update({"fun": ret_["fun"]})
        if "user" in ret_:
            load.update({"user": ret_["user"]})

    # Try to reach returner methods
    try:
        savefstr_func = mminion.returners[savefstr]
        getfstr_func = mminion.returners[getfstr]
        fstr_func = mminion.returners[fstr]
    except KeyError as error:
        emsg = "Returner '{}' does not support function {}".format(job_cache, error)
        log.error(emsg)
        raise KeyError(emsg)

    save_load = True
    if job_cache == "local_cache" and mminion.returners[getfstr](load.get("jid", "")):
        # The job was saved previously.
        save_load = False

    if save_load:
        try:
            mminion.returners[savefstr](load["jid"], load)
        except KeyError as e:
            log.error("Load does not contain 'jid': %s", e)
        except Exception:  # pylint: disable=broad-except
            log.critical(
                "The specified '%s' returner threw a stack trace",
                job_cache,
                exc_info=True,
            )

    try:
        mminion.returners[fstr](load)
    except Exception:  # pylint: disable=broad-except
        log.critical(
            "The specified '%s' returner threw a stack trace", job_cache, exc_info=True
        )

    if opts.get("job_cache_store_endtime") and updateetfstr in mminion.returners:
        mminion.returners[updateetfstr](load["jid"], endtime)


def store_minions(opts, jid, minions, mminion=None, syndic_id=None):
    """
    Store additional minions matched on lower-level masters using the configured
    master_job_cache
    """
    if mminion is None:
        mminion = salt.minion.MasterMinion(opts, states=False, rend=False)
    job_cache = opts["master_job_cache"]
    minions_fstr = "{}.save_minions".format(job_cache)

    try:
        mminion.returners[minions_fstr](jid, minions, syndic_id=syndic_id)
    except KeyError:
        raise KeyError(
            "Returner '{}' does not support function save_minions".format(job_cache)
        )


def get_retcode(ret):
    """
    Determine a retcode for a given return
    """
    retcode = 0
    # if there is a dict with retcode, use that
    if isinstance(ret, dict) and ret.get("retcode", 0) != 0:
        return ret["retcode"]
    # if its a boolean, False means 1
    elif isinstance(ret, bool) and not ret:
        return 1
    return retcode


def get_keep_jobs_seconds(opts):
    """
    Temporary function until 'keep_jobs' is fully deprecated,
    this will prefer 'keep_jobs_seconds', and only use
    'keep_jobs' as the configuration value if 'keep_jobs_seconds'
    is unmodified, and 'keep_jobs' is modified (in which case it
    will emit a deprecation warning).
    """
    keep_jobs_seconds = opts.get("keep_jobs_seconds", 86400)
    keep_jobs = opts.get("keep_jobs", 24)
    if keep_jobs_seconds == 86400 and keep_jobs != 24:
        salt.utils.versions.warn_until(
            "Argon",
            "The 'keep_jobs' option has been deprecated and replaced with "
            "'keep_jobs_seconds'.",
        )
        keep_jobs_seconds = keep_jobs * 3600
    return keep_jobs_seconds
