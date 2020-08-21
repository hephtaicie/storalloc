
import os
import random
import stat
import string
import unittest
import nvmet.nvme as nvme

# Default test devices are ram disks, but allow user to specify different
# block devices or files.
NVMET_TEST_DEVICES = os.getenv("NVMET_TEST_DEVICES",
                               "/dev/ram0,/dev/ram1").split(',')


def test_devices_present():
    return len([x for x in NVMET_TEST_DEVICES
                if os.path.exists(x) and
                (stat.S_ISBLK(os.stat(x).st_mode) or os.path.isfile(x))]) >= 2


class TestNvmet(unittest.TestCase):
    def test_subsystem(self):
        root = nvme.Root()
        root.clear_existing()
        for s in root.subsystems:
            self.assertTrue(False, 'Found Subsystem after clear')

        # create mode
        s1 = nvme.Subsystem(nqn='testnqn1', mode='create')
        self.assertIsNotNone(s1)
        self.assertEqual(len(list(root.subsystems)), 1)

        # any mode, should create
        s2 = nvme.Subsystem(nqn='testnqn2', mode='any')
        self.assertIsNotNone(s2)
        self.assertEqual(len(list(root.subsystems)), 2)

        # random name
        s3 = nvme.Subsystem(mode='create')
        self.assertIsNotNone(s3)
        self.assertEqual(len(list(root.subsystems)), 3)

        # duplicate
        self.assertRaises(nvme.CFSError, nvme.Subsystem,
                          nqn='testnqn1', mode='create')
        self.assertEqual(len(list(root.subsystems)), 3)

        # lookup using any, should not create
        s = nvme.Subsystem(nqn='testnqn1', mode='any')
        self.assertEqual(s1, s)
        self.assertEqual(len(list(root.subsystems)), 3)

        # lookup only
        s = nvme.Subsystem(nqn='testnqn2', mode='lookup')
        self.assertEqual(s2, s)
        self.assertEqual(len(list(root.subsystems)), 3)

        # lookup without nqn
        self.assertRaises(nvme.CFSError, nvme.Subsystem, mode='lookup')

        # and delete them all
        for s in root.subsystems:
            s.delete()
        self.assertEqual(len(list(root.subsystems)), 0)

    def test_namespace(self):
        root = nvme.Root()
        root.clear_existing()

        s = nvme.Subsystem(nqn='testnqn', mode='create')
        for n in s.namespaces:
            self.assertTrue(False, 'Found Namespace in new Subsystem')

        # create mode
        n1 = nvme.Namespace(s, nsid=3, mode='create')
        self.assertIsNotNone(n1)
        self.assertEqual(len(list(s.namespaces)), 1)

        # any mode, should create
        n2 = nvme.Namespace(s, nsid=2, mode='any')
        self.assertIsNotNone(n2)
        self.assertEqual(len(list(s.namespaces)), 2)

        # create without nsid, should pick lowest available
        n3 = nvme.Namespace(s, mode='create')
        self.assertIsNotNone(n3)
        self.assertEqual(n3.nsid, 1)
        self.assertEqual(len(list(s.namespaces)), 3)

        n4 = nvme.Namespace(s, mode='create')
        self.assertIsNotNone(n4)
        self.assertEqual(n4.nsid, 4)
        self.assertEqual(len(list(s.namespaces)), 4)

        # duplicate
        self.assertRaises(nvme.CFSError, nvme.Namespace, 1, mode='create')
        self.assertEqual(len(list(s.namespaces)), 4)

        # lookup using any, should not create
        n = nvme.Namespace(s, nsid=3, mode='any')
        self.assertEqual(n1, n)
        self.assertEqual(len(list(s.namespaces)), 4)

        # lookup only
        n = nvme.Namespace(s, nsid=2, mode='lookup')
        self.assertEqual(n2, n)
        self.assertEqual(len(list(s.namespaces)), 4)

        # lookup without nsid
        self.assertRaises(nvme.CFSError, nvme.Namespace, None, mode='lookup')

        # and delete them all
        for n in s.namespaces:
            n.delete()
        self.assertEqual(len(list(s.namespaces)), 0)

    @unittest.skipUnless(test_devices_present(),
                         "Devices %s not available or suitable" % ','.join(NVMET_TEST_DEVICES))
    def test_namespace_attrs(self):
        root = nvme.Root()
        root.clear_existing()

        s = nvme.Subsystem(nqn='testnqn', mode='create')
        n = nvme.Namespace(s, mode='create')

        self.assertFalse(n.get_enable())
        self.assertTrue('device' in n.attr_groups)
        self.assertTrue('path' in n.list_attrs('device'))

        # no device set yet, should fail
        self.assertRaises(nvme.CFSError, n.set_enable, 1)

        # now set a path and enable
        n.set_attr('device', 'path', NVMET_TEST_DEVICES[0])
        n.set_enable(1)
        self.assertTrue(n.get_enable())

        # test double enable
        n.set_enable(1)

        # test that we can't write to attrs while enabled
        self.assertRaises(nvme.CFSError, n.set_attr, 'device', 'path',
                          NVMET_TEST_DEVICES[1])
        self.assertRaises(nvme.CFSError, n.set_attr, 'device', 'nguid',
                          '15f7767b-50e7-4441-949c-75b99153dea7')

        # disable: once and twice
        n.set_enable(0)
        n.set_enable(0)

        # enable again, and remove while enabled
        n.set_enable(1)
        n.delete()

    def test_recursive_delete(self):
        root = nvme.Root()
        root.clear_existing()

        s = nvme.Subsystem(nqn='testnqn', mode='create')
        n1 = nvme.Namespace(s, mode='create')
        n2 = nvme.Namespace(s, mode='create')

        s.delete()
        self.assertEqual(len(list(root.subsystems)), 0)

    def test_port(self):
        root = nvme.Root()
        root.clear_existing()
        for p in root.ports:
            self.assertTrue(False, 'Found Port after clear')

        # create mode
        p1 = nvme.Port(portid=0, mode='create')
        self.assertIsNotNone(p1)
        self.assertEqual(len(list(root.ports)), 1)

        # any mode, should create
        p2 = nvme.Port(portid=1, mode='any')
        self.assertIsNotNone(p2)
        self.assertEqual(len(list(root.ports)), 2)

        # duplicate
        self.assertRaises(nvme.CFSError, nvme.Port,
                          portid=0, mode='create')
        self.assertEqual(len(list(root.ports)), 2)

        # lookup using any, should not create
        p = nvme.Port(portid=0, mode='any')
        self.assertEqual(p1, p)
        self.assertEqual(len(list(root.ports)), 2)

        # lookup only
        p = nvme.Port(portid=1, mode='lookup')
        self.assertEqual(p2, p)
        self.assertEqual(len(list(root.ports)), 2)

        # and delete them all
        for p in root.ports:
            p.delete()
        self.assertEqual(len(list(root.ports)), 0)

    def test_loop_port(self):
        root = nvme.Root()
        root.clear_existing()

        s = nvme.Subsystem(nqn='testnqn', mode='create')
        p = nvme.Port(portid=0, mode='create')

        # subsystem doesn't exists, should fail
        self.assertRaises(nvme.CFSError, p.add_subsystem, 'invalidnqn')

        self.assertTrue('addr' in p.attr_groups)

        # no trtype set yet, should fail
        self.assertRaises(nvme.CFSError, p.add_subsystem, 'testnqn')

        # now set trtype to loop and other attrs and enable
        p.set_attr('addr', 'trtype', 'loop')
        p.set_attr('addr', 'adrfam', 'ipv4')
        p.set_attr('addr', 'traddr', '192.168.0.1')
        p.set_attr('addr', 'treq', 'not required')
        p.set_attr('addr', 'trsvcid', '1023')
        p.add_subsystem('testnqn')

        # test double add
        self.assertRaises(nvme.CFSError, p.add_subsystem, 'testnqn')

        # test that we can't write to attrs while enabled
        self.assertRaises(nvme.CFSError, p.set_attr, 'addr', 'trtype',
                          'rdma')
        self.assertRaises(nvme.CFSError, p.set_attr, 'addr', 'adrfam',
                          'ipv6')
        self.assertRaises(nvme.CFSError, p.set_attr, 'addr', 'traddr',
                          '10.0.0.1')
        self.assertRaises(nvme.CFSError, p.set_attr, 'addr', 'treq',
                          'required')
        self.assertRaises(nvme.CFSError, p.set_attr, 'addr', 'trsvcid',
                          '21')

        # remove: once and twice
        p.remove_subsystem('testnqn')
        self.assertRaises(nvme.CFSError, p.remove_subsystem, 'testnqn')

        # check that the attrs haven't been tampered with
        self.assertEqual(p.get_attr('addr', 'trtype'), 'loop')
        self.assertEqual(p.get_attr('addr', 'adrfam'), 'ipv4')
        self.assertEqual(p.get_attr('addr', 'traddr'), '192.168.0.1')
        self.assertEqual(p.get_attr('addr', 'treq'), 'not required')
        self.assertEqual(p.get_attr('addr', 'trsvcid'), '1023')

        # add again, and try to remove while enabled
        p.add_subsystem('testnqn')
        p.delete()

    def test_host(self):
        root = nvme.Root()
        root.clear_existing()
        for p in root.hosts:
            self.assertTrue(False, 'Found Host after clear')

        # create mode
        h1 = nvme.Host(nqn='foo', mode='create')
        self.assertIsNotNone(h1)
        self.assertEqual(len(list(root.hosts)), 1)

        # any mode, should create
        h2 = nvme.Host(nqn='bar', mode='any')
        self.assertIsNotNone(h2)
        self.assertEqual(len(list(root.hosts)), 2)

        # duplicate
        self.assertRaises(nvme.CFSError, nvme.Host,
                          'foo', mode='create')
        self.assertEqual(len(list(root.hosts)), 2)

        # lookup using any, should not create
        h = nvme.Host('foo', mode='any')
        self.assertEqual(h1, h)
        self.assertEqual(len(list(root.hosts)), 2)

        # lookup only
        h = nvme.Host('bar', mode='lookup')
        self.assertEqual(h2, h)
        self.assertEqual(len(list(root.hosts)), 2)

        # and delete them all
        for h in root.hosts:
            h.delete()
        self.assertEqual(len(list(root.hosts)), 0)

    def test_referral(self):
        root = nvme.Root()
        root.clear_existing()

        # create port
        p = nvme.Port(portid=1, mode='create')
        self.assertEqual(len(list(p.referrals)), 0)

        # create mode
        r1 = nvme.Referral(p, name="1", mode='create')
        self.assertIsNotNone(r1)
        self.assertEqual(len(list(p.referrals)), 1)

        # any mode, should create
        r2 = nvme.Referral(p, name="2", mode='any')
        self.assertIsNotNone(r2)
        self.assertEqual(len(list(p.referrals)), 2)

        # duplicate
        self.assertRaises(nvme.CFSError, nvme.Referral,
                          p, name="2", mode='create')
        self.assertEqual(len(list(p.referrals)), 2)

        # lookup using any, should not create
        r = nvme.Referral(p, name="1", mode='any')
        self.assertEqual(r1, r)
        self.assertEqual(len(list(p.referrals)), 2)

        # lookup only
        r = nvme.Referral(p, name="2", mode='lookup')
        self.assertEqual(r2, r)
        self.assertEqual(len(list(p.referrals)), 2)

        # non-existant lookup
        self.assertRaises(nvme.CFSError, nvme.Referral, p, name="foo",
                          mode='lookup')

        # basic state
        self.assertTrue('addr' in r.attr_groups)
        self.assertFalse(r.get_enable())

        # now set trtype to loop and other attrs and enable
        r.set_attr('addr', 'trtype', 'loop')
        r.set_attr('addr', 'adrfam', 'ipv4')
        r.set_attr('addr', 'traddr', '192.168.0.1')
        r.set_attr('addr', 'treq', 'not required')
        r.set_attr('addr', 'trsvcid', '1023')
        r.set_enable(1)

        # test double enable
        r.set_enable(1)

        # test that we can't write to attrs while enabled
        self.assertRaises(nvme.CFSError, r.set_attr, 'addr', 'trtype',
                          'rdma')
        self.assertRaises(nvme.CFSError, r.set_attr, 'addr', 'adrfam',
                          'ipv6')
        self.assertRaises(nvme.CFSError, r.set_attr, 'addr', 'traddr',
                          '10.0.0.1')
        self.assertRaises(nvme.CFSError, r.set_attr, 'addr', 'treq',
                          'required')
        self.assertRaises(nvme.CFSError, r.set_attr, 'addr', 'trsvcid',
                          '21')

        # disable: once and twice
        r.set_enable(0)
        r.set_enable(0)

        # check that the attrs haven't been tampered with
        self.assertEqual(r.get_attr('addr', 'trtype'), 'loop')
        self.assertEqual(r.get_attr('addr', 'adrfam'), 'ipv4')
        self.assertEqual(r.get_attr('addr', 'traddr'), '192.168.0.1')
        self.assertEqual(r.get_attr('addr', 'treq'), 'not required')
        self.assertEqual(r.get_attr('addr', 'trsvcid'), '1023')

        # enable again, and try to remove while enabled
        r.set_enable(1)
        r.delete()

        # remove the other one while disabled:
        r1.delete()
        self.assertEqual(len(list(p.referrals)), 0)

    def test_allowed_hosts(self):
        root = nvme.Root()

        h = nvme.Host(nqn='hostnqn', mode='create')

        s = nvme.Subsystem(nqn='testnqn', mode='create')

        # add allowed_host
        s.add_allowed_host(nqn='hostnqn')

        # duplicate
        self.assertRaises(nvme.CFSError, s.add_allowed_host, 'hostnqn')

        # invalid
        self.assertRaises(nvme.CFSError, s.add_allowed_host, 'invalid')

        # remove again
        s.remove_allowed_host('hostnqn')

        # duplicate removal
        self.assertRaises(nvme.CFSError, s.remove_allowed_host, 'hostnqn')

        # invalid removal
        self.assertRaises(nvme.CFSError, s.remove_allowed_host, 'foobar')

    def test_invalid_input(self):
        root = nvme.Root()
        root.clear_existing()

        self.assertRaises(nvme.CFSError, nvme.Subsystem,
                          nqn='', mode='create')
        self.assertRaises(nvme.CFSError, nvme.Subsystem,
                          nqn='/', mode='create')

        for l in [ 257, 512, 1024, 2048 ]:
            toolong = ''.join(random.choice(string.ascii_lowercase)
                              for i in range(l))
            self.assertRaises(nvme.CFSError, nvme.Subsystem,
                              nqn=toolong, mode='create')

        discover_nqn = "nqn.2014-08.org.nvmexpress.discovery"
        self.assertRaises(nvme.CFSError, nvme.Subsystem,
                          nqn=discover_nqn, mode='create')

        self.assertRaises(nvme.CFSError, nvme.Port,
                          portid=1 << 17, mode='create')

    @unittest.skipUnless(test_devices_present(),
                         "Devices %s not available or suitable" % ','.join(
                             NVMET_TEST_DEVICES))
    def test_save_restore(self):
        root = nvme.Root()
        root.clear_existing()

        h = nvme.Host(nqn='hostnqn', mode='create')

        s = nvme.Subsystem(nqn='testnqn', mode='create')
        s.add_allowed_host(nqn='hostnqn')

        s2 = nvme.Subsystem(nqn='testnqn2', mode='create')
        s2.set_attr('attr', 'allow_any_host', 1)

        n = nvme.Namespace(s, nsid=42, mode='create')
        n.set_attr('device', 'path', NVMET_TEST_DEVICES[0])
        n.set_enable(1)

        nguid = n.get_attr('device', 'nguid')

        p = nvme.Port(portid=66, mode='create')
        p.set_attr('addr', 'trtype', 'loop')
        p.set_attr('addr', 'adrfam', 'ipv4')
        p.set_attr('addr', 'traddr', '192.168.0.1')
        p.set_attr('addr', 'treq', 'not required')
        p.set_attr('addr', 'trsvcid', '1023')
        p.add_subsystem('testnqn')

        # save, clear, and restore
        root.save_to_file('test.json')
        root.clear_existing()
        root.restore_from_file('test.json')

        # additional restores should fai
        self.assertRaises(nvme.CFSError, root.restore_from_file,
                          'test.json', False)

        # ... unless forced!
        root.restore_from_file('test.json', True)

        # rebuild our view of the world
        h = nvme.Host(nqn='hostnqn', mode='lookup')
        s = nvme.Subsystem(nqn='testnqn', mode='lookup')
        s2 = nvme.Subsystem(nqn='testnqn2', mode='lookup')
        n = nvme.Namespace(s, nsid=42, mode='lookup')
        p = nvme.Port(portid=66, mode='lookup')

        self.assertEqual(s.get_attr('attr', 'allow_any_host'), "0")
        self.assertEqual(s2.get_attr('attr', 'allow_any_host'), "1")
        self.assertIn('hostnqn', s.allowed_hosts)

        # and check everything is still the same
        self.assertTrue(n.get_enable())
        self.assertEqual(n.get_attr('device', 'path'), NVMET_TEST_DEVICES[0])
        self.assertEqual(n.get_attr('device', 'nguid'), nguid)

        self.assertEqual(p.get_attr('addr', 'trtype'), 'loop')
        self.assertEqual(p.get_attr('addr', 'adrfam'), 'ipv4')
        self.assertEqual(p.get_attr('addr', 'traddr'), '192.168.0.1')
        self.assertEqual(p.get_attr('addr', 'treq'), 'not required')
        self.assertEqual(p.get_attr('addr', 'trsvcid'), '1023')
        self.assertIn('testnqn', p.subsystems)
        self.assertNotIn('testtnqn2', p.subsystems)
